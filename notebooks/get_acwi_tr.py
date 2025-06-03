#!/usr/bin/env python
"""
Fetches ACWI total-return series back to 1987 and saves two files:
  • acwi_tr_daily.csv
  • acwi_tr_monthly.csv
The pre-2008 gap is filled with the MSCI ACWI Net USD index.
"""
import pandas as pd
import yfinance as yf

# --- Extra imports for Stooq fallback ----------------------------------------
import requests
from io import StringIO
from pathlib import Path
from datetime import date
from typing import cast


# --- Helper: robust Investing.com fetch --------------------------------------
def fetch_investing_series(
    search_string: str, from_date: str = "01/01/1987", to_date: str = "31/12/2007"
):
    """
    Locate an index on Investing.com via investpy.search_quotes() and return a
    pandas Series of the Close prices.
    """
    import investpy

    try:
        # 1) Find the instrument(s)
        hits = investpy.search_quotes(
            text=search_string, products=["indices"], n_results=1
        )
        if isinstance(hits, list):
            if not hits:
                raise RuntimeError(
                    f"Could not find '{search_string}' on Investing.com. Try adjusting the search string or check available indices."
                )
            hit = hits[0]
        else:
            hit = hits
        # 2) Retrieve historical data (daily by default)
        df = cast(
            pd.DataFrame,
            hit.retrieve_historical_data(from_date=from_date, to_date=to_date),
        )  # type: ignore[attr-defined]
        return df["Close"].rename("ACWI_TR").sort_index()
    except Exception as e:
        print(f"Error fetching '{search_string}' from Investing.com: {e}")
        print(
            "Check the index name, date range, or your internet connection. You can also use investpy.indices.get_indices_list() to see available indices."
        )
        raise


# --- Helper: fallback fetch from Stooq ---------------------------------------
def fetch_stooq_series(
    symbol: str = "^awcitr", from_date: str = "1987-01-01", to_date: str = "2007-12-31"
):
    """
    Retrieve a daily series from Stooq’s free CSV endpoint.  Stooq tickers for
    total‑return indices generally end in 'tr' (e.g. '^spxtr' for the S&P 500).
    For MSCI ACWI Net TR USD the code '^awcitr' usually works; add or adjust
    symbols if needed.
    """
    url = f"https://stooq.com/q/d/l/?s={symbol.lower()}&i=d"
    try:
        csv = requests.get(url, timeout=15)
        csv.raise_for_status()
        df = pd.read_csv(StringIO(csv.text))
        df["Date"] = pd.to_datetime(df["Date"])
        df = df.set_index("Date").sort_index().loc[from_date:to_date]
        if df.empty or "Close" not in df:
            raise ValueError("No data returned")
        return df["Close"].rename("ACWI_TR")
    except Exception as e:
        raise RuntimeError(f"Error fetching '{symbol}' from Stooq: {e}")


# ----------------------- user settings ---------------------------------------
DATA_DIR = Path("data")  # where the CSVs will live
FIRST_DATE = "1987-01-01"  # earliest proxy history you care about
# -----------------------------------------------------------------------------

DATA_DIR.mkdir(exist_ok=True)

# 1 ── pull ACWI ETF history (prices *and* dividends)
acwi = yf.Ticker("ACWI")
hist = acwi.history(start=FIRST_DATE, end=date.today(), actions=True, auto_adjust=False)

# Ensure hist is a DataFrame and has required columns
if (
    hist is None
    or not isinstance(hist, pd.DataFrame)
    or "Close" not in hist
    or "Dividends" not in hist
):
    raise ValueError(
        "Failed to fetch ACWI history or missing required columns 'Close'/'Dividends'. Check your internet connection and yfinance availability."
    )

price = hist["Close"]
divs = hist["Dividends"]

# convert to total-return index (base = first price)
factor = 1 + (divs / price.shift())
factor.iloc[0] = 1.0
tr_etf = (factor.cumprod() * price.iloc[0]).rename("ACWI_TR")

# 2 ── pull MSCI ACWI Net USD index (or close alternative) as proxy (via Investing.com)
proxy_search_names = [
    "MSCI ACWI Net USD",
    "MSCI ACWI",
    "MSCI All Country World Net USD",
    "MSCI ACWI NET",
]

proxy_tr = None
for _name in proxy_search_names:
    try:
        proxy_tr = fetch_investing_series(
            search_string=_name, from_date="01/01/1987", to_date="31/12/2007"
        )
        print(f"Fetched proxy series using '{_name}'")
        break
    except Exception as _e:
        print(f"Attempt with '{_name}' failed: {_e}")
        continue

# --- Fallback 2: try Stooq if Investing.com blocked -------------------------
if proxy_tr is None:
    stooq_symbols = ["^awcitr", "^acwi", "acwi"]
    for _sym in stooq_symbols:
        try:
            proxy_tr = fetch_stooq_series(
                symbol=_sym, from_date="1987-01-01", to_date="2007-12-31"
            )
            print(f"Fetched proxy series from Stooq using '{_sym}'")
            break
        except Exception as _e:
            print(f"Attempt with Stooq '{_sym}' failed: {_e}")
            continue

if proxy_tr is None:
    raise RuntimeError(
        "Unable to retrieve a suitable MSCI ACWI proxy from Investing.com. "
        "Check available indices via investpy.indices.get_indices_list() or "
        "update the `proxy_search_names` list with a valid index name, "
        "or try a Stooq symbol such as '^awcitr'."
    )

# 3 ── splice proxy → ETF at 2008-03 (ETF inception)
handover = "2008-03-31"  # last trading day of first full month
scale = tr_etf.loc[handover] / proxy_tr.loc[handover]
proxy_scaled = proxy_tr.mul(scale)

acwi_tr_daily = (
    pd.concat(
        [
            proxy_scaled.loc[:handover],  # proxy up to hand-over
            tr_etf.loc[handover:],
        ]  # ETF thereafter
    )
    .sort_index()
    .asfreq("B")  # business-day frequency
)

# 4 ── resample to month-end
acwi_tr_monthly = acwi_tr_daily.resample("M").last()

# 5 ── save
acwi_tr_daily.to_csv(DATA_DIR / "acwi_tr_daily.csv", float_format="%.6f")
acwi_tr_monthly.to_csv(DATA_DIR / "acwi_tr_monthly.csv", float_format="%.6f")

print(
    "Done!  Rows saved:",
    len(acwi_tr_daily),
    "daily   |",
    len(acwi_tr_monthly),
    "monthly",
)
