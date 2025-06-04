import yfinance as yf

# 1. Configuration
# ───────────────────────────────────────────────────────────────
TICKER = "BNDX"
START_DATE = "2013-06-04"  # ETF inception date (first full trading day)
END_DATE = None  # None means “up to today”

df = yf.download(
    TICKER, start=START_DATE, end=END_DATE, progress=False, auto_adjust=False
)

if df is None or df.empty:
    raise ValueError(
        f"No data downloaded for {TICKER} from {START_DATE} to {END_DATE or 'today'}. Check ticker symbol and date range."
    )

# Save to CSV
outfile = f"{TICKER}_raw.csv"
df.to_csv(outfile)
print(f"✅  Saved {len(df):,} rows to {outfile}")
