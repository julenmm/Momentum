import pandas as pd
import duckdb
from pathlib import Path

# Pathing: The database is in the ../database folder relative to this script
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "database" / "market_data.db"

def get_connection():
    """Returns a read-only connection to the DuckDB database."""
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found at {DB_PATH}. Please run the downloaders first.")
    return duckdb.connect(str(DB_PATH), read_only=True)

def load_ticker_data(ticker: str) -> pd.DataFrame:
    """
    Loads historical price and return data for a specific ticker.
    
    Returns:
        pd.DataFrame: DataFrame with Date as index.
    """
    con = get_connection()
    try:
        query = "SELECT * FROM full_history WHERE ticker = ? ORDER BY date"
        df = con.execute(query, (ticker.upper(),)).df()
        
        if df.empty:
            print(f"Warning: No data found for ticker {ticker}")
            return df
            
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        return df
    finally:
        con.close()

def load_all_tickers_data(column: str = 'daily_return', only_available: bool|None = None) -> pd.DataFrame:
    """
    Loads a specific column for all available tickers in a wide format.
    Default is 'daily_return'.
    
    Args:
        column (str): The column to pivot (e.g., 'daily_return', 'adj_close').
        
    Returns:
        pd.DataFrame: Wide-format DataFrame (Index=Date, Columns=Tickers).
    """
    con = get_connection()
    try:
        # We fetch only necessary columns for the wide-format pivot
        if only_available is None or not only_available: 
            query = f"SELECT date, ticker, {column} FROM full_history ORDER BY date"
        else:
            query = f"SELECT date, ticker, {column} FROM full_history WHERE ticker IN (SELECT ticker FROM successful_tickers) ORDER BY date"
        df = con.execute(query).df()
        
        if df.empty:
            return df
            
        df['date'] = pd.to_datetime(df['date'])
        
        # Pivot the data to wide format
        pivot_df = df.pivot(index='date', columns='ticker', values=column)
        return pivot_df
    finally:
        con.close()

def get_available_tickers() -> list[str]:
    """Returns a list of all tickers currently stored in the database."""
    con = get_connection()
    try:
        tickers = con.execute("SELECT ticker FROM successful_tickers ORDER BY ticker").df()
        return tickers['ticker'].tolist()
    finally:
        con.close()
