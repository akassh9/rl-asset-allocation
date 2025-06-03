#!/usr/bin/env python3
"""
Script to download iShares ACWI ETF historical data from Yahoo Finance
and save it as “ACWI_ETF.csv” with Date and Adj Close columns.

Dependencies:
    pip install yfinance pandas
"""

import yfinance as yf

# 1. Configuration
# ───────────────────────────────────────────────────────────────
TICKER = "ACWI"
ETF_CSV = "ACWI_ETF.csv"
START_DATE = "2008-03-26"  # ETF inception date (first full trading day)
END_DATE = None  # None means “up to today”


# 2. Download the data
# ───────────────────────────────────────────────────────────────
# The .download() method fetches daily OHLC + Volume + Adj Close by default.
df = yf.download(
    TICKER, start=START_DATE, end=END_DATE, progress=False, auto_adjust=False
)

# Check if data was downloaded successfully
if df is None or df.empty:
    raise ValueError(
        f"No data downloaded for {TICKER} from {START_DATE} to {END_DATE or 'today'}. Check ticker symbol and date range."
    )

# 3. Keep only the “Adj Close” column and write to CSV
# ───────────────────────────────────────────────────────────────
# This CSV will have a Date index and a single column “Adj Close”:
df[["Adj Close"]].to_csv(ETF_CSV, header=True)

print(f"Saved {TICKER} history to “{ETF_CSV}” (from {START_DATE} to present).")
