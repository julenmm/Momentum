import wrds
import os
from dotenv import load_dotenv
import duckdb
from pathlib import Path

# OS-agnostic pathing: The project root is one level up from this script's directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_DIR = PROJECT_ROOT / "database"
DB_PATH = DB_DIR / "market_data.db"

load_dotenv()

START_DATE = "1950-01-01"
END_DATE = "2026-01-01"


db_wrds = wrds.Connection(
    wrds_username=os.getenv("WRDS_USER"), wrds_password=os.getenv("WRDS_PASSWORD")
)

db_duckdb = duckdb.connect(str(DB_PATH))

# print("Libraries available with wrds:", db.list_libraries())

# print("Tables available in crsp library:", db.list_tables(library="crsp"))

# structure = db_wrds.describe_table(library="crsp", table="dsedelist")
# print("table structure", structure)

# NYSE/AMEX/NASDAQ STOCKS in d.hexcd IN (1, 2, 3);
QUERY_DAILY_RETURNS_WITH_TICKER_INFO = f"""
SELECT 
    d.permno as crsp_company_id, 
    n.ticker, 
    n.comnam as company_name, 
    d.date, 
    d.ret as returns, 
    d.prc as price,
    d.hexcd as exchange_code
FROM crsp.dsf AS d
LEFT JOIN crsp.stocknames AS n 
    ON d.permno = n.permno 
    AND d.date BETWEEN n.namedt AND n.nameenddt
WHERE d.date >= '{START_DATE}'
AND d.hexcd IN (1, 2, 3);
"""


# Query to get P/E and P/B ratios for a specific timeframe
def ratio_query(crsp_ids: list):
    """
    Generates a SQL query to fetch financial ratios for specific CRSP IDs.

    Args:
        crsp_ids_str (str): Comma-separated string of CRSP permno IDs.

    Returns:
        str: The SQL query string.
    """
    id_string = ", ".join(map(str, crsp_ids))
    return f"""
    SELECT permno as crsp_company_id, public_date, pe_exi as pe_ratio, bm as book_to_market_ratio, roe, capital_ratio, ps as price_to_sales_ratio
    FROM wrdsapps_finratio.firm_ratio
    WHERE public_date >= '{START_DATE}'
    AND permno IN ({id_string});
    """


def load_crsp_daily_returns():
    """
    Downloads daily stock returns from WRDS (CRSP library) based on the predefined query.

    Returns:
        pd.DataFrame: A DataFrame containing the daily returns data.
    """
    df = db_wrds.raw_sql(QUERY_DAILY_RETURNS_WITH_TICKER_INFO)
    return df


def get_all_downloaded_crsp_ids():
    """
    Retrieves a list of all unique CRSP company IDs already stored in the local DuckDB database.
    Returns an empty list if the table does not exist.

    Returns:
        list: A list of distinct CRSP company IDs.
    """
    df = (
        db_duckdb.execute("SELECT DISTINCT permno FROM crsp_daily_returns")
        .fetchdf()
        .permno.tolist()
    )
    return df


def save_crsp_daily_returns(df):
    """
    Saves the provided daily returns DataFrame to a DuckDB table, overwriting existing data.

    Args:
        df (pd.DataFrame): The DataFrame to save.
    """
    db_duckdb.execute("DROP TABLE IF EXISTS crsp_daily_returns")
    db_duckdb.execute("CREATE TABLE crsp_daily_returns AS SELECT * FROM df")
    db_duckdb.commit()


def load_crsp_ratios():
    """
    Downloads firm ratios from WRDS based on the IDs available in the local database.

    Returns:
        pd.DataFrame: A DataFrame containing financial ratios.
    """
    crsp_ids = get_all_downloaded_crsp_ids()
    df = db_wrds.raw_sql(ratio_query(crsp_ids))
    return df


def save_crsp_ratios(df):
    """
    Saves the provided ratios DataFrame to a DuckDB table, overwriting existing data.

    Args:
        df (pd.DataFrame): The DataFrame to save.
    """
    db_duckdb.execute("DROP TABLE IF EXISTS crsp_ratios")
    db_duckdb.execute("CREATE TABLE crsp_ratios AS SELECT * FROM df")
    db_duckdb.commit()


def format_crsp_daily_returns_for_usage():
    """
    Formats the CRSP daily returns table for usage in the rest of the pipeline.
    """
    db_duckdb.execute("ALTER TABLE crsp_daily_returns DROP COLUMN IF EXISTS log_return")
    db_duckdb.execute("ALTER TABLE crsp_daily_returns ADD COLUMN log_return DOUBLE")
    db_duckdb.execute(
        """UPDATE crsp_daily_returns 
    SET log_return = 
        CASE 
            WHEN returns IS NULL THEN NULL
            WHEN returns = -1 THEN -99.999999
            WHEN (1 + returns) > 0 THEN LN(1 + returns)
            ELSE -99.999999
        END; 
        """
    )
    db_duckdb.commit()


def fetch_delisting_returns():
    """
    Downloads delisting returns from WRDS (CRSP library) based on the predefined query.

    Returns:
        pd.DataFrame: A DataFrame containing the delisting returns data.
    """
    QUERY_DELISTING_RETURNS = f"""
            SELECT
                permno as crsp_company_id,
                dlstdt as date,
                dlret,
                dlretx,
                dlstcd,
                dlprc
            FROM crsp.dsedelist
            WHERE dlstdt >= '{START_DATE}'
        """
    df = db_wrds.raw_sql(QUERY_DELISTING_RETURNS)
    return df


def save_crsp_delisting_returns(df):
    """
    Saves the provided delisting returns DataFrame to a DuckDB table, overwriting existing data.

    Args:
        df (pd.DataFrame): The DataFrame to save.
    """
    db_duckdb.execute("DROP TABLE IF EXISTS crsp_delistings")
    db_duckdb.execute("CREATE TABLE crsp_delistings AS SELECT * FROM df")
    db_duckdb.commit()


def merge_delisting_returns():
    """
    Merges delisting returns into the main daily returns table and creates a
    survivorship-aware total return field.

    Notes:
    - returns_total combines regular CRSP return and delisting return when both exist:
        (1 + ret) * (1 + dlret) - 1
    - invalid values that would make log(1+r) impossible are left as NULL
      (you can handle full wipeouts later in the portfolio simulator if desired)
    """
    db_duckdb.execute(
        """
        CREATE OR REPLACE TABLE final_crsp_daily_returns AS
        SELECT
            d.*,
            dl.dlret,
            dl.dlretx,
            dl.dlstcd,
            dl.dlprc,

            CASE
                -- combine daily + delisting return when both are present and mathematically valid
                WHEN d.returns IS NOT NULL
                 AND dl.dlret IS NOT NULL
                 AND (1 + d.returns) > 0
                 AND (1 + dl.dlret) >= 0
                THEN ((1 + d.returns) * (1 + dl.dlret) - 1)

                -- only regular return available
                WHEN d.returns IS NOT NULL
                 AND (1 + d.returns) > 0
                THEN d.returns

                -- only delisting return available
                WHEN dl.dlret IS NOT NULL
                 AND (1 + dl.dlret) >= 0
                THEN dl.dlret

                ELSE NULL
            END AS returns_total,

            -- optional convenience column for downstream backtests
            CASE
                WHEN
                    CASE
                        WHEN d.returns IS NOT NULL
                         AND dl.dlret IS NOT NULL
                         AND (1 + d.returns) > 0
                         AND (1 + dl.dlret) >= 0
                        THEN ((1 + d.returns) * (1 + dl.dlret) - 1)

                        WHEN d.returns IS NOT NULL
                         AND (1 + d.returns) > 0
                        THEN d.returns

                        WHEN dl.dlret IS NOT NULL
                         AND (1 + dl.dlret) >= 0
                        THEN dl.dlret

                        ELSE NULL
                    END IS NULL
                THEN NULL

                WHEN 1 + (
                    CASE
                        WHEN d.returns IS NOT NULL
                         AND dl.dlret IS NOT NULL
                         AND (1 + d.returns) > 0
                         AND (1 + dl.dlret) >= 0
                        THEN ((1 + d.returns) * (1 + dl.dlret) - 1)

                        WHEN d.returns IS NOT NULL
                         AND (1 + d.returns) > 0
                        THEN d.returns

                        WHEN dl.dlret IS NOT NULL
                         AND (1 + dl.dlret) >= 0
                        THEN dl.dlret

                        ELSE NULL
                    END
                ) > 0
                THEN LN(
                    1 + (
                        CASE
                            WHEN d.returns IS NOT NULL
                             AND dl.dlret IS NOT NULL
                             AND (1 + d.returns) > 0
                             AND (1 + dl.dlret) >= 0
                            THEN ((1 + d.returns) * (1 + dl.dlret) - 1)

                            WHEN d.returns IS NOT NULL
                             AND (1 + d.returns) > 0
                            THEN d.returns

                            WHEN dl.dlret IS NOT NULL
                             AND (1 + dl.dlret) >= 0
                            THEN dl.dlret

                            ELSE NULL
                        END
                    )
                )
                ELSE NULL
            END AS log_return

        FROM crsp_daily_returns d
        LEFT JOIN crsp_delistings dl
          ON d.crsp_company_id = dl.crsp_company_id
         AND d.date = dl.date
        """
    )
    db_duckdb.commit()


def run():
    """
    Main execution function: loads daily returns from WRDS and saves them to DuckDB.
    """

    # df = load_crsp_daily_returns()
    # save_crsp_daily_returns(df)
    # format_crsp_daily_returns_for_usage()

    # df = fetch_delisting_returns()
    # save_crsp_delisting_returns(df)
    # merge_delisting_returns()

    # df = load_crsp_ratios()
    # save_crsp_ratios(df)

    print("=" * 50)
    print("Number of companies with daily returns data downloaded:")
    print(
        db_duckdb.execute(
            "SELECT COUNT(*) FROM crsp_daily_returns GROUP BY crsp_company_id"
        ).fetchdf()
    )
    print("=" * 50)
    print("Number of companies with ratios data downloaded:")
    print(
        db_duckdb.execute(
            "SELECT COUNT(*) FROM crsp_ratios GROUP BY crsp_company_id"
        ).fetchdf()
    )
    print("=" * 50)
    print("Number of companies with delisting returns data downloaded:")
    print(
        db_duckdb.execute(
            "SELECT COUNT(*) FROM crsp_delistings GROUP BY crsp_company_id"
        ).fetchdf()
    )
    print("=" * 50)
    print("Number of companies with all returns data downloaded:")
    print(
        db_duckdb.execute(
            "SELECT COUNT(*) FROM final_crsp_daily_returns GROUP BY crsp_company_id"
        ).fetchdf()
    )
    print("=" * 50)

    print("Columns for crsp_daily_returns:")
    print(db_duckdb.execute("SELECT * FROM crsp_daily_returns LIMIT 1").fetchdf())
    print("=" * 50)
    print("Columns for crsp_ratios:")
    print(db_duckdb.execute("SELECT * FROM crsp_ratios LIMIT 1").fetchdf())
    print("=" * 50)
    print("Columns for crsp_delistings:")
    print(db_duckdb.execute("SELECT * FROM crsp_delistings LIMIT 1").fetchdf())
    print("=" * 50)
    print("columns for final_crsp_daily_returns:")
    print(db_duckdb.execute("SELECT * FROM final_crsp_daily_returns LIMIT 1").fetchdf())
    print("=" * 50)


if __name__ == "__main__":
    run()


"""
Columns for crsp_daily_returns:
   crsp_company_id ticker               company_name        date  returns   price  exchange_code  log_return
==================================================
Columns for crsp_ratios:
   crsp_company_id public_date   pe_ratio  book_to_market_ratio       roe  capital_ratio  price_to_sales_ratio
==================================================
"""
