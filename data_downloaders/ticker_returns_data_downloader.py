import pandas as pd
import duckdb
import time
from pathlib import Path
from yfinance_api import fetch_ticker_data

Y_FINANCE_RATE_LIMIT = 0.5

# OS-agnostic pathing: The project root is one level up from this script's directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent
TICKER_DATA_PATH = PROJECT_ROOT / "TickerData" / "US-Stock-Symbols-main" / "all" / "all_tickers.txt"
DB_DIR = PROJECT_ROOT / "database"
DB_PATH = DB_DIR / "market_data.db"

class Controller:
    def __init__(self):
        self.db_path = DB_PATH
        self.ticker_path = TICKER_DATA_PATH
        self.init_db()

    def init_db(self):
        """Initializes the DuckDB database and ensures directories exist."""
        DB_DIR.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(self.db_path))
        
        # Create tables if they don't exist
        # We use snake_case for consistency in SQL
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS full_history (
                date TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                adj_close DOUBLE,
                daily_return DOUBLE,
                log_return DOUBLE,
                ticker VARCHAR
            )
        """)
        self.con.execute("CREATE TABLE IF NOT EXISTS successful_tickers (ticker VARCHAR PRIMARY KEY)")
        self.con.execute("CREATE TABLE IF NOT EXISTS failed_tickers (ticker VARCHAR PRIMARY KEY, error VARCHAR)")

    def get_tickers(self):
        """Reads tickers from the text file."""
        if not self.ticker_path.exists():
            print(f"Error: Ticker file not found at {self.ticker_path}")
            return []
        
        with open(self.ticker_path, "r") as f:
            tickers = [line.strip() for line in f if line.strip()]
        return tickers

    def run(self):
        tickers = self.get_tickers()
        print(f"Starting download for {len(tickers)} tickers...")

        for ticker in tickers:
            # Check if already processed to avoid duplicates
            already_done = self.con.execute(
                "SELECT 1 FROM successful_tickers WHERE ticker = ?", (ticker,)
            ).fetchone()
            if already_done:
                print(f"Skipping {ticker}, already in database.")
                continue

            try:
                # Fetch as far back as possible using default "max" logic in yfinance_api
                df = fetch_ticker_data(ticker)
                
                if df is not None and not df.empty:
                    # Prepare for DuckDB: add ticker column and reset index to get Date
                    df = df.reset_index().rename(columns={"index": "date"})
                    df["ticker"] = ticker
                    
                    # Store in DuckDB
                    # DuckDB can register pandas dataframes automatically
                    self.con.execute("INSERT INTO full_history SELECT * FROM df")
                    self.con.execute("INSERT OR IGNORE INTO successful_tickers VALUES (?)", (ticker,))
                    print(f"Saved {len(df)} rows for {ticker}")
                else:
                    self.con.execute("INSERT OR IGNORE INTO failed_tickers VALUES (?, 'Empty result')", (ticker,))
                
            except Exception as e:
                print(f"Critical error fetching {ticker}: {e}")
                self.con.execute("INSERT OR IGNORE INTO failed_tickers VALUES (?, ?)", (ticker, str(e)))

            # Rate limiting
            time.sleep(Y_FINANCE_RATE_LIMIT)

        print("Data download completed.")
        self.con.close()

class Main:
    def run(self):
        controller = Controller()
        controller.run()

if __name__ == "__main__":
    main = Main()
    main.run()