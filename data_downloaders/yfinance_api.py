import datetime as dt
import random
import time
import requests
import pandas as pd
import numpy as np

_YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"


def _to_utc_epoch(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.timezone.utc)
        return int(value.timestamp())
    if isinstance(value, dt.date):
        value = dt.datetime.combine(value, dt.time.min, tzinfo=dt.timezone.utc)
        return int(value.timestamp())

    ts = pd.to_datetime(value)
    if getattr(ts, "tzinfo", None) is None:
        ts = ts.tz_localize("UTC")
    return int(ts.timestamp())


def _fetch_chart_json(ticker, params, max_retries=5, base_sleep=2):
    url = _YAHOO_CHART_URL.format(ticker=ticker)
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
    }

    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=20)
            if resp.status_code in (429, 502, 503):
                sleep_s = base_sleep * (2 ** attempt) + random.uniform(0, 1)
                time.sleep(sleep_s)
                continue
            resp.raise_for_status()
            data = resp.json()
            error = data.get("chart", {}).get("error")
            if error:
                raise RuntimeError(error.get("description") or str(error))
            return data
        except (requests.RequestException, ValueError):
            if attempt == max_retries - 1:
                raise
            sleep_s = base_sleep * (2 ** attempt) + random.uniform(0, 1)
            time.sleep(sleep_s)

    return None


def _chart_json_to_df(data):
    if not data:
        return pd.DataFrame()

    result = data.get("chart", {}).get("result") or []
    if not result:
        return pd.DataFrame()

    payload = result[0]
    timestamps = payload.get("timestamp") or []
    if not timestamps:
        return pd.DataFrame()

    index = pd.to_datetime(timestamps, unit="s", utc=True).tz_convert(None)
    indicators = payload.get("indicators", {})
    quote = (indicators.get("quote") or [{}])[0]
    adjclose = (indicators.get("adjclose") or [{}])[0].get("adjclose")

    df = pd.DataFrame(
        {
            "Open": quote.get("open"),
            "High": quote.get("high"),
            "Low": quote.get("low"),
            "Close": quote.get("close"),
            "Volume": quote.get("volume"),
        },
        index=index,
    )

    if adjclose:
        df["Adj Close"] = adjclose
    else:
        df["Adj Close"] = df["Close"]

    df = df[~df.index.duplicated(keep="first")].sort_index()
    return df

def fetch_ticker_data(ticker, start_date=None, end_date=None, chunk_days=None, interval="1d"):
    print(f"Requesting {ticker}...", end=" ")
    time.sleep(2)

    if end_date is None:
        end_date = dt.date.today()

    try:
        if start_date is None:
            if end_date is None:
                params = {"range": "max", "interval": interval, "events": "div,splits"}
            else:
                params = {
                    "period1": 0,
                    "period2": _to_utc_epoch(end_date),
                    "interval": interval,
                    "events": "div,splits",
                }

            data = _fetch_chart_json(ticker, params)
            df = _chart_json_to_df(data)
        else:
            if chunk_days is None:
                params = {
                    "period1": _to_utc_epoch(start_date),
                    "period2": _to_utc_epoch(end_date),
                    "interval": interval,
                    "events": "div,splits",
                }
                data = _fetch_chart_json(ticker, params)
                df = _chart_json_to_df(data)
            else:
                frames = []
                cur = pd.to_datetime(start_date)
                end = pd.to_datetime(end_date)
                delta = pd.Timedelta(days=chunk_days)

                while cur < end:
                    nxt = min(cur + delta, end)
                    params = {
                        "period1": _to_utc_epoch(cur),
                        "period2": _to_utc_epoch(nxt),
                        "interval": interval,
                        "events": "div,splits",
                    }
                    data = _fetch_chart_json(ticker, params)
                    part = _chart_json_to_df(data)
                    if not part.empty:
                        frames.append(part)
                    cur = nxt
                    time.sleep(2)

                df = pd.concat(frames) if frames else pd.DataFrame()
                if not df.empty:
                    df = df[~df.index.duplicated(keep="first")].sort_index()

        if not df.empty:
            df["Daily_Return"] = df["Adj Close"].pct_change()
            df["Log_Return"] = np.log(df["Adj Close"]).diff()
            print("[✓] Success")
            return df

        print("[!] Empty dataframe")
        return None
    except Exception as e:
        print(f"\n[!] Failed to retrieve {ticker}: {e}")
        return None

