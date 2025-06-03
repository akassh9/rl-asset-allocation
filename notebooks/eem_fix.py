import pandas as pd

price = pd.read_csv(
    "../data/processed/eem_spliced_monthly_price.csv", parse_dates=["Date"]
)
price["spliced_price"] = price["index_price"].combine_first(price["EEM"])
price = price[["Date", "spliced_price"]]
price.to_csv("../data/processed/eem_spliced_monthly_price.csv", index=False)

price["spliced_return"] = price["spliced_price"].pct_change()
price[["Date", "spliced_return"]].to_csv(
    "../data/processed/eem_spliced_monthly_returns.csv", index=False
)
