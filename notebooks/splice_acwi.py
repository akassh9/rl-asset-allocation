#!/usr/bin/env python3
"""
Splice MSCI ACWI Net-Total-Return index (“^ACWI”) onto the iShares ACWI ETF
(“ACWI”) at the *return* level:

• Index:   1988-01-01 → 2008-03-31   (keeps the March-2008 return)
• ETF:     2008-04-30 → today        (from first month with a valid return)

Outputs
-------
ACWI_spliced_monthly_returns.csv   # contiguous monthly total-return series
ACWI_spliced_monthly_prices.csv    # cosmetic continuous price (scaled index → ETF)
"""

import pandas as pd
import pandas.tseries.offsets as off

# ─── 1. CONFIG ────────────────────────────────────────────────────────────────
IDX_CSV = "../data/raw/MSCI-ACWI.csv"  # Yahoo ticker ^ACWI (index, adj close)
ETF_CSV = "../data/raw/ACWI_ETF.csv"  # Yahoo ticker ACWI  (ETF,  adj close)

RET_CSV = "ACWI_spliced_monthly_returns.csv"
PRC_CSV = "ACWI_spliced_monthly_prices.csv"

CUT_DATE = pd.Timestamp("2008-03-31")  # last index month
START_ETF_MTD = CUT_DATE + off.MonthEnd(1)  # first ETF month (= 2008-04-30)

# ─── 2. LOAD DATA ─────────────────────────────────────────────────────────────
idx = pd.read_csv(
    IDX_CSV, parse_dates=["Date"], index_col="Date", usecols=["Date", "Adj Close"]
).sort_index()

etf = pd.read_csv(
    ETF_CSV, parse_dates=["Date"], index_col="Date", usecols=["Date", "Adj Close"]
).sort_index()

# ─── 3. MONTH-END PRICES ──────────────────────────────────────────────────────
idx_px = idx["Adj Close"].resample("ME").last()
etf_px = etf["Adj Close"].resample("ME").last()

# ─── 4. MONTHLY RETURNS ───────────────────────────────────────────────────────
idx_ret = idx_px.pct_change().dropna()
etf_ret = etf_px.pct_change().dropna()

spliced_ret = pd.concat(
    [idx_ret.loc[:CUT_DATE], etf_ret.loc[START_ETF_MTD:]]
).sort_index()

# ─── 5. COSMETIC PRICE SERIES (OPTIONAL) ──────────────────────────────────────
factor = etf_px.loc[CUT_DATE] / idx_px.loc[CUT_DATE]
idx_scaled_px = idx_px * factor

spliced_px = pd.concat(
    [idx_scaled_px.loc[:CUT_DATE], etf_px.loc[START_ETF_MTD:]]
).sort_index()

# remove any accidental duplicate month-end (shouldn’t exist, but safe)
spliced_px = spliced_px[~spliced_px.index.duplicated(keep="first")]

# ─── 6. SAVE OUTPUTS ──────────────────────────────────────────────────────────
spliced_ret.to_csv(RET_CSV, header=["Monthly Return"])
spliced_px.to_csv(PRC_CSV, header=["Spliced Price"])

# ─── 7. SUMMARY ───────────────────────────────────────────────────────────────
print("=== ACWI splice summary ===")
print(f"Index returns: {idx_ret.index.min().date()}  →  {idx_ret.index.max().date()}")
print(f"ETF returns:   {etf_ret.index.min().date()}  →  {etf_ret.index.max().date()}")
print(
    f"Spliced:       {spliced_ret.index.min().date()}  →  {spliced_ret.index.max().date()}"
)
print(f"Months in series: {len(spliced_ret)}")
print("===========================")
