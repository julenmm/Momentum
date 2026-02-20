import pandas as pd
import pandas_datareader.data as web
import duckdb
from datetime import datetime, timedelta
from pathlib import Path

# -------------------------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------------------------

# OS-agnostic pathing: The project root is one level up from this script's directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_DIR = PROJECT_ROOT / "database"
DB_PATH = DB_DIR / "market_data.db"

# REPLACE THIS with your actual free API key from: https://fred.stlouisfed.org/docs/api/api_key.html
FRED_API_KEY = '13f76cd19e7c0580183a6eb841f0503b' 
START_DATE = datetime.now() - timedelta(days=10*365) # Fetch 10 years by default
END_DATE = datetime.now()

FRED_SERIES_DICT = {
    "M2 Money Supply (Seasonally Adj)": "M2SL",
    "Real GDP": "GDPC1",
    "10Y-2Y Treasury Yield Spread": "T10Y2Y",
    "Non-Farm Payrolls": "PAYEMS",
    "Initial Jobless Claims": "ICSA",
    "High Yield Corp Bond Yield (Junk Proxy)": "BAMLH0A0HYM2", 
    "AAA Corp Bond Yield": "AAA",
    "University of Michigan: Consumer Sentiment": "UMCSENT",
}

# -------------------------------------------------------------------------
# DATABASE INITIALIZATION
# -------------------------------------------------------------------------

def init_db():
    DB_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    
    # Table for time-series data
    con.execute("""
        CREATE TABLE IF NOT EXISTS economic_data (
            date TIMESTAMP,
            value DOUBLE,
            series_id VARCHAR
        )
    """)
    
    # Table for series metadata
    con.execute("""
        CREATE TABLE IF NOT EXISTS economic_series_names (
            series_id VARCHAR PRIMARY KEY,
            series_name VARCHAR
        )
    """)
    return con

# -------------------------------------------------------------------------
# FUNCTIONS
# -------------------------------------------------------------------------

def fetch_and_store_fred_data(con, series_dict, api_key):
    print(f"--- Fetching/Storing Macro Data from FRED ---")
    for name, series_id in series_dict.items():
        try:
            print(f"Requesting {name} ({series_id})...", end=" ")
            df = web.DataReader(series_id, 'fred', START_DATE, END_DATE, api_key=api_key)
            
            if not df.empty:
                # 1. Update metadata table
                con.execute("""
                    INSERT OR REPLACE INTO economic_series_names (series_id, series_name) 
                    VALUES (?, ?)
                """, (series_id, name))

                # 2. Prepare time-series data
                store_df = df.reset_index().rename(columns={"DATE": "date", df.columns[0]: "value"})
                store_df["series_id"] = series_id
                
                # 3. Store in economic_data (Delete existing for this series to avoid duplicates)
                con.execute("DELETE FROM economic_data WHERE series_id = ?", (series_id,))
                con.execute("INSERT INTO economic_data SELECT date, value, series_id FROM store_df")
                
                print(f"[✓] Success: {len(df)} records stored")
            else:
                print(f"[!] Empty data returned")
                
        except Exception as e:
            print(f"\n[!] Failed to retrieve {name}: {e}")

# -------------------------------------------------------------------------
# EXECUTION
# -------------------------------------------------------------------------

if __name__ == "__main__":
    if FRED_API_KEY == 'YOUR_FRED_API_KEY_HERE':
        print("WARNING: You are using a placeholder API key. FRED requests may fail.")
    
    conn = init_db()
    fetch_and_store_fred_data(conn, FRED_SERIES_DICT, FRED_API_KEY)
    
    print("\n--- Current Economic Series in Database ---")
    meta = conn.execute("SELECT * FROM economic_series_names").df()
    print(meta)
    
    conn.close()
    print("\nMacro download completed.")