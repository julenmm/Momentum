import pandas as pd
import datetime

# Assuming a query function exists or is imported from your db module
# from my_db_module import query


class MeanReversionPortfolio:
    """
    A class to construct and backtest a mean reversion-based portfolio using data from DuckDB.
    """

    def __init__(self):
        self.data: pd.DataFrame = None
        self.weights: pd.DataFrame = None
        self.performance: pd.DataFrame = None
        self.tickers: list = None
        self.percentile_results: dict = {}

    def fetch_data(
        self,
        tickers: list | pd.DataFrame,
        time_start: str = "2020-01-01",
        aggregate: str | None = None,
    ):
        """
        Fetches log returns from DuckDB.
        Safe to pass a List, DataFrame, or Series of tickers.
        """
        # --- FIX START: Ensure tickers is a standard Python list ---
        if isinstance(tickers, pd.DataFrame):
            # If DataFrame, take the first column
            self.tickers = tickers.iloc[:, 0].tolist()
        elif isinstance(tickers, pd.Series):
            self.tickers = tickers.tolist()
        else:
            self.tickers = list(tickers)
        # --- FIX END ---

        if self.tickers:
            # Fix: Format list as a tuple string for SQL 'IN' clause
            tickers_tuple = tuple(self.tickers)

            # Handle single item tuple trailing comma issue
            if len(self.tickers) == 1:
                tickers_str = f"('{self.tickers[0]}')"
            else:
                tickers_str = str(tickers_tuple)

            q = f"""
            SELECT date, log_return, ticker 
            FROM full_history 
            WHERE ticker in {tickers_str} 
            AND date > '{time_start}'
            """

            # Assuming query() returns a dataframe
            raw_data = query(q)

            # Pivot so index=date, columns=ticker
            self.data = raw_data.pivot(
                index="date", columns="ticker", values="log_return"
            )

            # Ensure index is datetime for resampling
            self.data.index = pd.to_datetime(self.data.index)

        if aggregate:
            self.data = self.data.resample(aggregate).sum()

    def compute_weights_last_month_direction(
        self,
        top_percentile: float,
        bottom_percentile: float = 0,
        weight_strategy: str = "CEW",
    ):
        if self.data is None:
            raise ValueError("Data not fetched. Call fetch_data() first.")

        rows = []

        # FIX: Ensure we correctly identify the start and end percentages
        # This handles inputs like [0, 10] or [10, 0] correctly
        pct_min = min(top_percentile, bottom_percentile)
        pct_max = max(top_percentile, bottom_percentile)

        for date, row in self.data.iterrows():
            # Sort: Index 0 is the Best performer (Highest Return)
            sorted_row = row.sort_values(ascending=False)

            l = len(sorted_row)

            # Calculate indices based on the sorted min/max percentages
            start_index = int(pct_min * l / 100)
            end_index = int(pct_max * l / 100)

            # Avoid division by zero if the slice is empty
            amount_of_holdings = max(1, end_index - start_index)
            ticker_weight = 1 / amount_of_holdings

            ticker_to_weight = {}

            # Only iterate through the slice we care about to save time
            # We convert the Series to a list of keys (tickers) to access by index
            sorted_tickers = sorted_row.index.tolist()

            # 1. Initialize all to 0
            # (Optional efficiency: only set the non-zero ones if you handle sparse data later,
            # but setting all to 0 is safer for full dataframes)
            ticker_to_weight = {ticker: 0.0 for ticker in sorted_tickers}

            # 2. Set the weights for the winners
            # We slice the list directly using start:end
            selected_tickers = sorted_tickers[start_index:end_index]
            for ticker in selected_tickers:
                ticker_to_weight[ticker] = ticker_weight

            ticker_to_weight["date"] = date
            rows.append(ticker_to_weight)

        self.weights = pd.DataFrame(rows)
        self.weights.set_index("date", inplace=True)

    def compute_weight_momentum_12_minus_1(
        self,
        top_percentile: float,
        bottom_percentile: float = 0,
        weight_strategy: str = "CEW",
    ):
        if self.data is None:
            raise ValueError("Data not fetched.")

        # --- STEP 1: Calculate the "12-1" Momentum Signal ---
        # 1. Calculate 12-month total return (rolling sum of log returns)
        # 2. Subtract the most recent month (1-month return) to remove "reversion" effect
        # This gives you the return from t-12 to t-1

        # Note: We use .shift(1) to ensure we use KNOWN data (no look-ahead bias)
        # But since we handle the shift in compute_index_level_and_returns, we just need the signal here.

        # Calculate standard 12-month return
        twelve_month_return = self.data.rolling(window=12).sum()

        # Calculate 1-month return (the most recent one)
        one_month_return = self.data.rolling(window=1).sum()

        # SIGNAL: 12-month return MINUS the last month (The "12-1" Strategy)
        momentum_signal = twelve_month_return - one_month_return

        rows = []

        # Fix: Handle [0, 10] vs [10, 0] input order
        pct_min = min(top_percentile, bottom_percentile)
        pct_max = max(top_percentile, bottom_percentile)

        # Iterate through the SIGNAL dataframe, not the raw daily/monthly returns
        for date, row_signal in momentum_signal.iterrows():
            # Skip rows where we don't have enough history yet (first 12 months)
            if row_signal.isnull().all():
                continue

            # Sort by the 12-1 Signal (Best past performance excluding last month)
            sorted_row = row_signal.sort_values(ascending=False)

            l = len(sorted_row.dropna())
            if l == 0:
                continue

            # Calculate slice indices
            start_index = int(pct_min * l / 100)
            end_index = int(pct_max * l / 100)

            amount_of_holdings = max(1, end_index - start_index)
            ticker_weight = 1 / amount_of_holdings

            sorted_tickers = sorted_row.index.tolist()

            # Initialize weights
            ticker_to_weight = {ticker: 0.0 for ticker in sorted_tickers}

            # Select winners based on the 12-1 signal
            selected_tickers = sorted_tickers[start_index:end_index]
            for ticker in selected_tickers:
                ticker_to_weight[ticker] = ticker_weight

            ticker_to_weight["date"] = date
            rows.append(ticker_to_weight)

        self.weights = pd.DataFrame(rows)
        self.weights.set_index("date", inplace=True)

    def compute_index_level_and_returns(self, start_index=100):
        """
        Computes the portfolio returns and index level.

        Crucial Step: Shifts weights by 1 period.
        Momentum observed at time T determines holdings for T+1.
        """
        if self.weights is None or self.data is None:
            raise ValueError("Weights or Data missing.")

        # 1. Shift weights forward by one period.
        # The weight calculated based on January data (row Jan) applies to February returns.
        effective_weights = self.weights.shift(1)

        # 2. Align DataFrames (ensure indices match)
        # Multiply weights by returns to get weighted return per asset
        weighted_returns = effective_weights * self.data

        # 3. Sum across columns (tickers) to get Portfolio Return for that period
        # Note: Summing log returns cross-sectionally is an approximation.
        # For rigorous geometric linking, convert to simple, sum, then back to log.
        # Here we follow the standard simple aggregation for the index.
        self.performance = pd.DataFrame()
        self.performance["portfolio_log_return"] = weighted_returns.sum(axis=1)

        # 4. Handle the start (first period has no weights from previous period)
        self.performance.dropna(inplace=True)

        # 5. Compute Index Level
        # Index_t = Index_{t-1} * exp(Return_t)  [if using log returns]
        # Or more simply: Cumulative Sum of log returns gives the total log return.

        # Calculate cumulative log return
        self.performance["cumulative_log_return"] = self.performance[
            "portfolio_log_return"
        ].cumsum()

        # Convert to Price Index
        # Index = Start_Value * e^(cumulative_log_return)
        import numpy as np

        self.performance["index_level"] = start_index * np.exp(
            self.performance["cumulative_log_return"]
        )

        return self.performance

    def construct_performances_for_percentiles(
        self, percentiles: list[tuple[float, float]]
    ):
        """
        Runs the backtest logic for multiple percentile combinations on the existing data.
        Stores results in self.percentile_results.

        Args:
            percentiles: List of tuples, e.g., [(10, 0), (20, 0), (50, 50)]
                         Format is (top_percentile, bottom_percentile)
        """
        if self.data is None:
            raise ValueError("Data not loaded. Please call fetch_data() first.")

        self.percentile_results = {}

        for top_p, bottom_p in percentiles:
            # 1. Compute weights for this specific configuration
            # This updates self.weights temporarily
            self.compute_weight_momentum_12_minus_1(
                top_percentile=top_p, bottom_percentile=bottom_p, weight_strategy="CEW"
            )

            # 2. Compute the returns and index level
            # This updates self.performance temporarily
            perf_df = self.compute_index_level_and_returns(start_index=100)

            # 3. Store a COPY of the results keyed by a descriptive string
            # We copy() so subsequent loops don't overwrite this dataframe in memory
            key_name = f"Top {top_p}% / Bottom {bottom_p}%"
            self.percentile_results[key_name] = perf_df.copy()

    def construct_portfolio_average_holding_period_returns(self):
        """
        Calculates the average log return per holding period for all stored strategies.
        """
        if not self.percentile_results:
            print(
                "No performances stored. Run construct_performances_for_percentiles() first."
            )
            return None

        stats = {}
        for name, df in self.percentile_results.items():
            # Calculate mean of the log returns column
            avg_return = df["portfolio_log_return"].mean()
            stats[name] = avg_return

        return pd.Series(stats, name="Average HPR (Log)")

    def construct_portfolio_average_standard_deviation_for_holding_period_return(self):
        """
        Calculates the standard deviation of returns for all stored strategies.
        """
        if not self.percentile_results:
            print(
                "No performances stored. Run construct_performances_for_percentiles() first."
            )
            return None

        stats = {}
        for name, df in self.percentile_results.items():
            # Calculate std dev of the log returns column
            std_dev = df["portfolio_log_return"].std()
            stats[name] = std_dev

        return pd.Series(stats, name="Std Dev HPR (Log)")

    def plot_portfolios_over_HPR(self):
        """
        Plots the index level (growth of $100) for all stored percentile strategies.
        """
        import matplotlib.pyplot as plt

        if not self.percentile_results:
            print("No performances to plot.")
            return

        plt.figure(figsize=(12, 6))

        for name, df in self.percentile_results.items():
            plt.plot(df.index, df["index_level"], label=name)

        plt.title("Momentum Portfolio Performance Comparison")
        plt.xlabel("Date")
        plt.ylabel("Index Level (Start=100)")
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.show()

    def plot_portfolio_average_returns(self):
        """
        Generates a bar chart comparing the average returns of all strategies.
        """
        import matplotlib.pyplot as plt

        # 1. Get the data using the function we defined earlier
        avg_returns = self.construct_portfolio_average_holding_period_returns()

        if avg_returns is None or avg_returns.empty:
            print("No data to plot.")
            return

        # 2. Create Plot
        plt.figure(figsize=(10, 6))

        # Plot bar chart
        avg_returns.plot(kind="bar", color="skyblue", alpha=0.8)

        plt.title("Average Holding Period Return by Strategy")
        plt.xlabel("Strategy (Percentiles)")
        plt.ylabel("Avg Log Return")
        plt.xticks(rotation=45, ha="right")
        plt.grid(axis="y", alpha=0.3)
        plt.tight_layout()
        plt.show()

    def plot_portfolio_standard_deviations(self):
        """
        Generates a bar chart comparing the volatility (std dev) of all strategies.
        """
        import matplotlib.pyplot as plt

        # 1. Get the data using the function we defined earlier
        std_devs = (
            self.construct_portfolio_average_standard_deviation_for_holding_period_return()
        )

        if std_devs is None or std_devs.empty:
            print("No data to plot.")
            return

        # 2. Create Plot
        plt.figure(figsize=(10, 6))

        # Plot bar chart
        std_devs.plot(kind="bar", color="salmon", alpha=0.8)

        plt.title("Volatility (Std Dev) by Strategy")
        plt.xlabel("Strategy (Percentiles)")
        plt.ylabel("Standard Deviation of Returns")
        plt.xticks(rotation=45, ha="right")
        plt.grid(axis="y", alpha=0.3)
        plt.tight_layout()
        plt.show()
