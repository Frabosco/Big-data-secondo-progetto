#!/usr/bin/env python3
"""reducer.py"""

import sys
import datetime
actions = {}
columns={"close":0,"low":1, "hight":2, "volume":3, "date":4}
tickerList={}

def update(key, year, close, date, low, hight, volume):
    if(date<tickerList[key][year]["ftClose"][1]):
        tickerList[key][year]["ftClose"]=(close,date)
    elif(date>tickerList[key][year]["lsClose"][1]):
        tickerList[key][year]["lsClose"]=(close,date)
    if low<tickerList[key][year]["min"]:
        tickerList[key][year]["min"]=low
    if hight>tickerList[key][year]["max"]:
        tickerList[key][year]["max"]=hight
    tickerList[key][year]["volume"].append(volume)


for line in sys.stdin:
    line = line.strip()
    if line:
        key, row = line.split("\t")
    else:
        break

    key=key.split("', '")
    key=[i.strip("'[]") for i in key]
    key=(key[0],key[1])
    row=row.split("', '")
    row=[i.strip("'[]\"") for i in row]

    close=float(row[columns["close"]])
    low=float(row[columns["low"]])
    hight=float(row[columns["hight"]])
    volume=float(row[columns["volume"]])
    date=row[columns["date"]]
    year=date[:4]
    date= datetime.datetime.strptime(date,"%Y-%m-%d")

    if key not in tickerList.keys():
        yearList={}
        yearList[year]={"ftClose":(close,date), "lsClose":(close,date), "min":low, "max":hight, "volume":[volume]}
        tickerList[key]=yearList

    else:
        if year not in tickerList[key].keys():
            tickerList[key][year]={"ftClose":(close,date), "lsClose":(close,date), 
                            "min":low, "max":hight, "volume":[volume]}
            
        else:
            update(key, year, close, date, low, hight, volume)

for key in tickerList.keys():
    print(f"ticker: {key[0]}, nome: {key[1]}, anni:")
    years=sorted(tickerList[key].keys())

    for year in years:
        diff=tickerList[key][year]["lsClose"][0]-tickerList[key][year]["ftClose"][0]
        diffP=diff/tickerList[key][year]["ftClose"][0]*100
        volumeMeans=sum(tickerList[key][year]["volume"])/len(tickerList[key][year]["volume"])
        print(f"\tanno: {year}, var percentuale: {int(diffP)}%,  min: {tickerList[key][year]["min"]:.2f}, max: {tickerList[key][year]["max"]:.2f}, volume medio: {int(volumeMeans)}")

    print("\n")