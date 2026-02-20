import pandas as pd
import numpy as np
from typing import Optional


def resample_returns(df: pd.DataFrame, freq: str, column_name: str) -> pd.DataFrame:
    """
    Resamples daily returns to a lower frequency by summing them.
    This assumes input returns are log returns. If they are simple returns,
    summing is an approximation (though commonly used for small returns).

    Args:
        df (pd.DataFrame): DataFrame with datetime index.
        freq (str): Resampling frequency (e.g., 'M', 'Q', '6M', 'Y').
        column_name (str): Column to resample THESE MUST BE LOG RETURNS

    Returns:
        pd.DataFrame: Resampled DataFrame.
    """
    return df[[column_name]].resample(freq).sum()


def resample_prices(
    df: pd.DataFrame,
    freq: str,
    price_col: str = "adj_close",
    return_type: Optional[str] = "log",
    output_col: Optional[str] = None,
) -> pd.DataFrame:
    """
    Resamples price data to a lower frequency (last observation) and optionally
    calculates returns between those periods.

    Args:
        df (pd.DataFrame): DataFrame containing price data with a datetime index.
        freq (str): Resampling frequency (e.g., 'M' for monthly, 'Q' for quarterly, '6M' for semi-annual, 'Y' for yearly).
        price_col (str): The name of the column containing the adjusted price.
        return_type (str, optional): Type of return to calculate: 'log', 'simple', or None. Defaults to 'log'.
        output_col (str, optional): Name for the resulting return column. If None, defaults to '{price_col}_{return_type}_return'.

    Returns:
        pd.DataFrame: A DataFrame with the resampled prices and/or returns.
    """
    # Resample to get the last price of the period
    resampled = df[[price_col]].resample(freq).last()

    if return_type is None:
        return resampled

    if output_col is None:
        output_col = f"{price_col}_{return_type}_return"

    if return_type == "log":
        resampled[output_col] = np.log(
            resampled[price_col] / resampled[price_col].shift(1)
        )
    elif return_type == "simple":
        resampled[output_col] = resampled[price_col].pct_change()
    else:
        raise ValueError("return_type must be 'log', 'simple', or None")

    return resampled


def daily_returns_to_monthly(
    df: pd.DataFrame, column_name: str = "log_returns"
) -> pd.DataFrame:
    """
    Convenience function to resample daily log returns to monthly.

    Args:
        df (pd.DataFrame): Daily data.
        column_name (str): Name of the return column.

    Returns:
        pd.DataFrame: Monthly summed returns.
    """
    return resample_returns(df, "M", column_name)


def daily_returns_to_quarterly(
    df: pd.DataFrame, column_name: str = "log_returns"
) -> pd.DataFrame:
    """
    Convenience function to resample daily log returns to quarterly.

    Args:
        df (pd.DataFrame): Daily data.
        column_name (str): Name of the return column.

    Returns:
        pd.DataFrame: Quarterly summed returns.
    """
    return resample_returns(df, "Q", column_name)


def daily_returns_to_semi_annual(
    df: pd.DataFrame, column_name: str = "log_returns"
) -> pd.DataFrame:
    """
    Convenience function to resample daily log returns to semi-annual.

    Args:
        df (pd.DataFrame): Daily data.
        column_name (str): Name of the return column.

    Returns:
        pd.DataFrame: Semi-annual (6 months) summed returns.
    """
    return resample_returns(df, "6M", column_name)


def daily_returns_to_yearly(
    df: pd.DataFrame, column_name: str = "log_returns"
) -> pd.DataFrame:
    """
    Convenience function to resample daily log returns to yearly.

    Args:
        df (pd.DataFrame): Daily data.
        column_name (str): Name of the return column.

    Returns:
        pd.DataFrame: Yearly summed returns.
    """
    return resample_returns(df, "Y", column_name)
