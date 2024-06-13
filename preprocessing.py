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

len=len(result)

result[:len/5].to_csv("/home/francesco/mapreduce/input/data:5.txt")
result[:len/2].to_csv("/home/francesco/mapreduce/input/data:2.txt")

result.to_csv("/home/francesco/mapreduce/input/data.txt")

resultx2=result
resultx5=result

for i, row in result.iterrows():
    resultx2.loc[len(resultx2.index)]=row
    resultx2.loc[len(resultx2.index)]=row

    resultx5.loc[len(resultx5.index)]=row
    resultx5.loc[len(resultx5.index)]=row
    resultx5.loc[len(resultx5.index)]=row
    resultx5.loc[len(resultx5.index)]=row
    resultx5.loc[len(resultx5.index)]=row

resultx2.to_csv("/home/francesco/mapreduce/input/datax2.txt")
resultx5.to_csv("/home/francesco/mapreduce/input/datax5.txt")

