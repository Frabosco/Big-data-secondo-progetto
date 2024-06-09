#!/usr/bin/env python3
"""preprocessing.py"""

import pandas as pd

hStocks= pd.read_csv("/home/francesco/mapreduce/archive/historical_stocks.csv")

hStockPrices= pd.read_csv("/home/francesco/mapreduce/archive/historical_stock_prices.csv")

hStockPrices=hStockPrices.drop(columns=["open","adj_close"])
hStocks=hStocks.drop(columns=["exchange"])

hStocks["name"]=hStocks["name"].str.replace(",","")
hStocks["industry"]=hStocks["industry"].str.replace(",","")

result= pd.merge(hStocks, hStockPrices, how="outer", on="ticker")

result.dropna(subset=["date"], inplace=True)

result.to_csv("/home/francesco/mapreduce/input/data.txt")
