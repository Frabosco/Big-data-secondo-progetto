#!/usr/bin/env python3
"""reducer.py"""

import sys
import datetime
import math
actions = {}
columns={"close":0, "volume":1, "date":2}
industryList={}

def update(industry, year, ticker, close, date, volume):
    
    if(date<industryList[industry][year]["ftClose"][1]):
        industryList[industry][year]["ftClose"]=(close,date)
    elif(date>industryList[industry][year]["lsClose"][1]):
        industryList[industry][year]["lsClose"]=(close,date)
    if ticker not in industryList[industry][year].keys():
        industryList[industry][year]["ticker"][ticker]={"ftClose":(close,date), "lsClose":(close,date), "volume":volume}
    else:
        if date<industryList[industry][year]["ticker"][ticker]["ftClose"][1]:
            industryList[industry][year]["ticker"][ticker]["ftClose"]=(close,date)
        if date>industryList[industry][year]["ticker"][ticker]["lsClose"][1]:
            industryList[industry][year]["ticker"][ticker]["lsClose"]=(close,date)
        if volume>industryList[industry][year]["ticker"][ticker]["volume"]:
            industryList[industry][year]["ticker"][ticker]["volume"]=volume


for line in sys.stdin:
    line = line.strip()
    if line:
        key, row = line.split("\t")
    else:
        break

    key=key.split("', '")
    key=[i.strip("'[]") for i in key]
    ticker=key[0]
    name=key[1]
    sector=key[2]
    industry=key[3]
    key=(industry,sector)
    row=row.split("', '")
    row=[i.strip("'[]\"") for i in row]
    
    close=float(row[columns["close"]])
    volume=float(row[columns["volume"]])
    date=row[columns["date"]]
    year=date[:4]
    date= datetime.datetime.strptime(date,"%Y-%m-%d")

    if key not in industryList.keys():
        yearList={}
        tickerList={}
        tickerList[ticker]={"ftClose":(close,date), "lsClose":(close,date), "volume":volume}
        yearList[year]={"ftClose":(close,date), "lsClose":(close,date), "ticker":tickerList}
        industryList[key]=yearList

    else:
        if year not in industryList[key].keys():
            tickerList={}
            tickerList[ticker]={"ftClose":(close,date), "lsClose":(close,date), "volume":volume}
            industryList[key][year]={"ftClose":(close,date), "lsClose":(close,date), "ticker":tickerList}
            
        else:
            update(key, year, ticker, close, date, volume)

sectorList={}
sectorDiff={}
for key in industryList.keys():
    years=sorted(industryList[key].keys())
    s=f"industria: {key[0]}anni:\n"
    maxDiffY=-math.inf
    for year in years:
        diffY=industryList[key][year]["lsClose"][0]-industryList[key][year]["ftClose"][0]
        diffPY=diffY/industryList[key][year]["ftClose"][0]*100
        tickers=industryList[key][year]["ticker"]
        maxDiff=(0,-math.inf)
        maxVol=(0,0)
        for ticker in tickers.keys():
            diff=tickers[ticker]["lsClose"][0]-tickers[ticker]["ftClose"][0]
            diffP=diff/tickers[ticker]["ftClose"][0]*100
            if diffP>maxDiff[1]:
                maxDiff=(ticker, diffP)
            if tickers[ticker]["volume"]>maxVol[1]:
                maxVol=(ticker, tickers[ticker]["volume"])
        s+=f"anno: {year}, var percentuale: {int(diffPY)}%, moglior incremento:({maxDiff[0]},{maxDiff[1]:.2f}), maggior volume:({maxVol[0]},{maxVol[1]:.2f})\n"
        if maxDiff[1]>maxDiffY:
            maxDiffY=maxDiff[1]
    if key[1] not in sectorList.keys():
        sectorList[key[1]]=[s]
        sectorDiff[key[1]]=diffPY
    else:
        sectorList[key[1]].append(s)
        if maxDiffY>sectorDiff[key[1]]:
            sectorDiff[key[1]]=maxDiffY
    

sectorDiff2=sectorDiff
sectors=sorted(sectorDiff2.items(), key=lambda x:x[1])

for sector in sectors:
    print(f"settore: {sector[0]}({sectorDiff[sector[0]]})")
    for l in sectorList[sector[0]]: 
        print (l)