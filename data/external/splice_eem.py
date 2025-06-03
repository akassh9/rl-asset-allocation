import pandas as pd
from pandas.tseries.offsets import MonthEnd

# ---------- 1. Read the raw files ----------
idx = pd.read_csv("msci_em_index.csv")
etf = pd.read_csv("eem_monthly_returns.csv")  # rename if needed

# ---------- 2. Parse dates ----------
idx["Date"] = pd.to_datetime(idx["Date"], format="%m/%Y") + MonthEnd(0)
etf["Date"] = pd.to_datetime(etf["Date"])

# ---------- 3. Build monthly index returns ----------
idx = idx.sort_values("Date")
idx["idx_ret"] = idx["index_price"].pct_change()

# ---------- 4. Align and (optionally) scale ----------
overlap = idx.merge(etf[["Date", "EEM"]], on="Date").dropna()
slope = (overlap["EEM"] * overlap["idx_ret"]).sum() / (overlap["idx_ret"] ** 2).sum()

# If you want exact matching, keep the next line; else comment it out.
idx["idx_ret"] *= slope

# ---------- 5. Splice ----------
cutoff = etf["Date"].min()
pre_etf = idx[idx["Date"] < cutoff][["Date", "idx_ret"]].rename(
    columns={"idx_ret": "EEM"}
)
combined = pd.concat([pre_etf, etf[["Date", "EEM"]]], ignore_index=True).sort_values(
    "Date"
)

# ---------- 6. Save ----------
combined.to_csv("EEM_spliced_monthly_returns.csv", index=False)
print("✅  Spliced file written to EEM_spliced_monthly_returns.csv")
