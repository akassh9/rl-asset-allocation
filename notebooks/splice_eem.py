#!/usr/bin/env python3
"""
splice_eem.py
Create a back-filled monthly price/return series for the iShares MSCI
Emerging Markets ETF (EEM) by splicing MSCI EM Index history before 2003-04.

Inputs  (expected paths)
-------
data/raw/msci_em_index.csv   # two columns: Date,<index level>

Outputs (written)
-------
data/eem_spliced_monthly_price.csv
data/eem_spliced_monthly_returns.csv
"""

from __future__ import annotations
from typing import cast
import pathlib
from typing import Tuple

import pandas as pd
import yfinance as yf

# -----------------------------------------------------------------------
# Paths & constants
# -----------------------------------------------------------------------
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
INPUT_DIR = PROJECT_ROOT / "data/raw"
OUTPUT_DIR = PROJECT_ROOT / "data/processed"
INDEX_CSV = INPUT_DIR / "msci_em_index.csv"
ETF_TICKER = "EEM"
START_DATE = "1987-01-01"  # pull a bit earlier than index start


# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------
def download_etf(ticker: str, start: str) -> pd.Series:
    """Return a Series of adjusted close prices for the ETF."""
    df = yf.download(ticker, start=start, progress=False, auto_adjust=False)

    # ---- Tell both the runtime *and* Pylance that df must be a DataFrame ----
    if df is None or df.empty:
        raise ValueError(f"No data returned for {ticker}")
    df = cast(pd.DataFrame, df)
    # ------------------------------------------------------------------------

    s = df["Adj Close"]
    s.name = "etf_price"
    return s


def load_index(path: pathlib.Path) -> pd.Series:
    """Load the MSCI index CSV and return it as a Series."""
    idx = pd.read_csv(path, parse_dates=["Date"])
    idx.set_index("Date", inplace=True)
    col = idx.columns[0]  # whatever the header is
    return idx[col].rename("index_price").astype(float)


def month_end(series: pd.Series) -> pd.Series:
    """Convert any frequency to month-end by taking the last value each month."""
    return series.resample("ME").last().dropna()


def splice_series(
    index_px: pd.Series, etf_px: pd.Series
) -> Tuple[pd.Series, pd.Series]:
    """
    Stitch together scaled index history (pre-ETF) with actual ETF prices.
    Returns (spliced_price, spliced_return).
    """
    idx_m = month_end(index_px)
    etf_m = month_end(etf_px)

    first_overlap = etf_m.index.min()
    scale_factor = etf_m.loc[first_overlap] / idx_m.loc[first_overlap]

    idx_scaled = idx_m * float(scale_factor)

    before_etf = idx_scaled[idx_scaled.index < first_overlap]
    after_etf = etf_m

    price = pd.concat([before_etf, after_etf]).sort_index()
    price.name = "spliced_price"
    ret = price.pct_change().dropna()
    ret.name = "spliced_return"

    return price, ret


# -----------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------
def main() -> None:
    OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

    etf_px = download_etf(ETF_TICKER, START_DATE)
    index_px = load_index(INDEX_CSV)

    price, ret = splice_series(index_px, etf_px)

    price.to_csv(OUTPUT_DIR / "eem_spliced_monthly_price.csv", header=True)
    ret.to_csv(OUTPUT_DIR / "eem_spliced_monthly_returns.csv", header=True)

    print("=== Splice Summary ===")
    print(f"Index data starts:       {price.index.min().date()}")
    print(f"ETF data starts:         {etf_px.index.min().date()}")
    print(
        f"Spliced returns cover:   {ret.index.min().date()} → {ret.index.max().date()}"
    )


if __name__ == "__main__":
    main()
