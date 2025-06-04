#!/usr/bin/env python3
"""
process_bndx.py

Convert daily BNDX data from yfinance into month-end adjusted-close prices
and corresponding month-over-month returns.
"""

import pandas as pd


def process_bndx(
    raw_csv: str,
    price_out: str = "BNDX_monthly_price.csv",
    ret_out: str = "BNDX_monthly_returns.csv",
) -> None:
    # ---- load & clean ----------------------------------------------------- #
    etf = pd.read_csv(raw_csv, parse_dates=["Date"])
    etf = (
        etf.set_index("Date")  # make Date the index
        .sort_index()  # ensure chronological order
        .loc[:, ["Adj Close"]]
    )  # keep only what we need

    # ---- month-end price series ------------------------------------------ #
    # Use 'M' (month-end).  In pandas ≥3.0 you can switch to 'ME'
    monthly_price = etf["Adj Close"].resample("ME").last()
    monthly_price.name = "Adj Close"

    # ---- month-over-month returns ---------------------------------------- #
    monthly_ret = monthly_price.pct_change().dropna()
    monthly_ret.name = "BNDX_ret"

    # ---- save ------------------------------------------------------------ #
    monthly_price.to_csv(price_out, float_format="%.6f")
    monthly_ret.to_csv(ret_out, float_format="%.8f")

    # quick sanity print
    # Ensure index is DatetimeIndex before calling .date()
    def _date_str(idx):
        if hasattr(idx, "date"):
            return idx.date()
        elif hasattr(idx, "strftime"):
            return idx.strftime("%Y-%m-%d")
        return str(idx)

    print(
        f"Monthly prices  : {price_out}  ({len(monthly_price)} rows, "
        f"{_date_str(monthly_price.index[0])} – {_date_str(monthly_price.index[-1])}"
    )
    print(
        f"Monthly returns : {ret_out}   ({len(monthly_ret)} rows, "
        f"{_date_str(monthly_ret.index[0])} – {_date_str(monthly_ret.index[-1])}"
    )


if __name__ == "__main__":
    process_bndx("BNDX_raw.csv")
