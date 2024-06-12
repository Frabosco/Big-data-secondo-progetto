#!/usr/bin/env python3
"""job2.py"""

import argparse
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

args = parser.parse_args()
inPath, outPath = args.input_path, args.output_path

spark = SparkSession \
    .builder \
    .appName("Spark job2") \
    .getOrCreate()

linesRDD=spark.sparkContext.textFile(inPath).cache()

sector2TickerRDD=linesRDD.flatMap(f=lambda action: [action.split(",")])


sector2TickerRDD=sector2TickerRDD.map(f=lambda action: (str(action[3])+","+str(action[4])+","+str(action[9][:4])+","+str(action[1]),[action[5], action[8], action[9]]))
schema=sector2TickerRDD.first()
sector2TickerRDD=sector2TickerRDD.filter(lambda line: line!=schema)

tickerFTRDD=sector2TickerRDD.reduceByKey(lambda x,y:x if x[2]<y[2] else y)
tickerLTRDD=sector2TickerRDD.reduceByKey(lambda x,y:x if x[2]>y[2] else y)
tickerVarRDD=tickerFTRDD.join(tickerLTRDD)
tickerVarRDD=tickerVarRDD.map(f=lambda action: (action[0],(float(action[1][1][0])-float(action[1][0][0]))/float(action[1][0][0])*100))


tickervolRDD=sector2TickerRDD.reduceByKey(lambda x,y:x if x[1]>y[1] else y)
tickervolRDD=tickervolRDD.map(f=lambda action: (action[0],action[1][1]))

tickerRDD=tickerVarRDD.join(tickervolRDD)

yearRDD=tickerRDD.map(f=lambda x: (x[0].split(","),x[1]))
yearRDD=yearRDD.map(f=lambda x: (str(x[0][0])+","+str(x[0][1])+","+str(x[0][2]),[x[0][3],x[1][0], x[1][1]]))

yearFT=yearRDD.reduceByKey(lambda x,y:x if x[1]>y[1] else y)
yearFT=yearFT.map(f=lambda x: (x[0],(x[1][0],x[1][1])))

yearRDD=yearFT.join(yearRDD)

industryRDD=yearRDD.map(f=lambda x:(x[0].split(","),x[1]))
industryRDD=industryRDD.map(f=lambda x: (str(x[0][0])+","+str(x[0][1]), [x[0][2], x[1][0], x[1][1][0], x[1][1][1], x[1][1][2]]))
industryRDD=industryRDD.groupByKey().mapValues(list).sortBy(lambda x: x[1][0][1])

sectorRDD=industryRDD.map(f=lambda x:(x[0].split(","),x[1]))
sectorRDD=sectorRDD.map(f=lambda x: (str(x[0][0]), (x[0][1], x[1])))
sectorRDD=sectorRDD.groupByKey().mapValues(list)

""" action2FT=action2TickerRDD.reduceByKey(lambda x,y:x if x[4]<y[4] else y)
action2LT=action2TickerRDD.reduceByKey(lambda x,y:x if x[4]>y[4] else y)
action2var=action2FT.join(action2LT)
action2var=action2var.map(f=lambda action: (action[0],(float(action[1][1][0])-float(action[1][0][0]))/float(action[1][0][0])*100))

action2min=action2TickerRDD.reduceByKey(lambda x,y:x if float(x[1])<float(y[1]) else y)
action2min=action2min.map(f=lambda action: (action[0],action[1][1]))
action2max=action2TickerRDD.reduceByKey(lambda x,y:x if float(x[1])>float(y[1]) else y)
action2max=action2max.map(f=lambda action: (action[0],action[1][2]))

action2avgV=action2TickerRDD.map(f=lambda x:(x[0],(float(x[1][3]),1)))
action2avgV=action2avgV.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]) )
action2avgV=action2avgV.map(f=lambda x:(x[0], float(x[1][0])/float(x[1][1])))
#print(action2FT.collect()[0])

yearRDD=action2var.join(action2min).join(action2max).join(action2avgV).sortByKey()
yearRDD=yearRDD.map(f=lambda action:(action[0].split(","),action[1]))
yearRDD=yearRDD.map(f=lambda action:(str(action[0][0])+","+str(action[0][1]),[action[0][2], action[1][0][0][0],action[1][0][0][1],action[1][0][1],action[1][1]]))
yearRDD=yearRDD.groupByKey().mapValues(list) """
sectorRDD.saveAsTextFile(outPath)

#Tiicker2YearRDD=action2TickerRDD.reduceBy()