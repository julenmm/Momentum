# Momentum Analytics Platform

Features:

### Data Downloader

- Ticker Returns Data Downloader: yahoo finance API
- Macro Data Downloader: FRED API (Federal Reserve Economic Data)

### Database

- DuckDB: High-performance database for storing and querying data.

### Analytics Pipeline

- Momentum Analytics Pipeline: This is the main pipeline that runs the analytics. It is a Jupyter notebook (`1_momentum_portfolios.ipynb`) that loads the data from the database and builds the portfolio.
  - **Features:**
    - Constructs portfolios based on 12-minus-1 month momentum.
    - Supports Weekly and Monthly rebalancing.
    - Generates performance metrics (returns, volatility) for decile buckets.
  - **LIMITATION:** The data relies on currently listed stocks from Yahoo Finance. This introduces **survivorship bias**, as delisted companies are not included. Results for "loser" portfolios may be inflated, and analysis is most relevant for recent periods where the listed universe is stable.

### Functions of `1_momentum_portfolios.ipynb`

This notebook is the core analytics engine. It performs the following steps based on the current execution parameters:

1.  **Data Ingestion & Filtering**:
    - Connects to the DuckDB database.
    - **Universe Definition**: Uses `tickers_with_last_10_years`, filtering for companies with a continuous 10-year history.
    - **Timeframe**: Analysis focuses on data starting from **2016-02-01**.
    - **Liquidity Filter**: Excludes stocks with an average price below $5 to avoid penny stock distortions.

2.  **Portfolio Construction**:
    - **Momentum Signal**: Calculates **12-minus-1 momentum** (12-month lookback, skipping the most recent month).
    - **Ranking & Bucketing**: Ranks tickers into **deciles** (10 buckets).
    - **Weighting**: Assigns equal weights (1/N) to all stocks within each decile.

3.  **Performance Analysis**:
    - **Rebalancing**: Runs backtests for both **Weekly** and **Monthly** rebalancing frequencies.
    - **Metrics**: Outputs Average Return, Standard Deviation, and approximate Sharpe Ratios for each decile.
    - **Visualization**: Plots cumulative index levels to compare the "Winner" (Top 10%) vs. "Loser" (Bottom 10%) strategies.

## System Requirements

- Python 3.10+ (Recommended: use an IDE like VS Code or PyCharm for notebooks instead of Anaconda, as this project uses recent Python features that may not be backward compatible).

## Setup Guide

### 1. Create a Virtual Environment

It's recommended to use a virtual environment to use the appropiate version of python and package versions

```bash
# Navigate to the project directory

# Create the virtual environment
python3 -m venv venv

# Activate the virtual environment
**You have to run this every time you open the project to use it.**
source venv/bin/activate
```

```bash
pip install -r requirements.txt
```

### 2. Enable Jupyter in the Virtual Environment (Analytics Module)

To use this virtual environment inside Jupyter Notebooks (like your `1_momentum_portfolios.ipynb`), you need to register it as a kernel:

```bash
# Register the kernel
python3 -m ipykernel install --user --name=venv --display-name "Python (Momentum Venv)"
```

Once done, when you open a `.ipynb` file in VS Code or Jupyter, select the kernel named **"Python (Momentum Venv)"**.

# How to use the app

this project has multiple functions. Every directory inside this project contains a fucntion of the project.They can be simplified to the following:

1. Data Downloader: This function is used to download the stock price data, economic data, and other data for downstream analysis. The sources are:

- Ticker Returns Data Downloader: yahoo finance API
- Macro Data Downloader: FRED API (Federal Reserve Economic Data)
  The FRED API requires an API key to access the data. but it is 100% free. You can get one here: https://fred.stlouisfed.org/docs/api/api_key.html

2. Database: This is where all the data downloaded is stored, and fecthed. We use duck db because it makes it very easy to query data without loading it in memory while running code on a local machine. It is the easiest to use self hosted database for Python.

## How to use Modules

### 1. Run the Downloader

#### Ticker Returns Data Downloader

Navigate to the root directory of the project.
Execute the main script to start populating your DuckDB database:

On Mac:

```bash
python3 data_downloaders/ticker_returns_data_downloader.py
```

On Windows:

```bash
python data_downloaders\\ticker_returns_data_downloader.py
```

#### Macro Data Downloader

Navigate to the root directory of the project.
Execute the main script to start populating your DuckDB database:

On Mac:

```bash
python3 data_downloaders/macro_data_downloader.py
```

On Windows:

```bash
python data_downloaders\\macro_data_downloader.py
```

## About the Project

## Data Storage

- Data is stored in `./duckdb/market_data.db`.
- The script automatically skips tickers that have already been saved.
- Failed attempts are logged in the `failed_tickers` table for review.

## Project Structure

- `data_downloader.py`: Main execution script.
- `yfinance_api.py`: Custom API for fetching data.
- `requirements.txt`: Project dependencies.
- `duckdb/`: Contains the generated database file.
- `TickerData/`: Source for all ticker symbols.

**Important Data Limitation (Survivorship Bias):**

I want to highlight a critical limitation regarding the data source. The project currently relies on publicly listed tickers available through Yahoo Finance. This means:

- **Survivorship Bias:** The dataset only includes companies that are currently listed. Stocks that have gone bankrupt, merged, or been delisted in the past are excluded.
- **Impact on Results:** This bias tends to inflate backtested performance, particularly for "loser" portfolios, as companies that would have caused significant losses are missing from the historical record.
- **Relevance:** The results should be interpreted with this caveat in mind. The strategy's performance on recent data is likely more indicative of real-world results than the long-term historical backtest.
