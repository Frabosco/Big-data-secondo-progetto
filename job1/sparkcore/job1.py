#!/usr/bin/env python3
"""job1.py"""

import argparse
from pyspark import SparkContext
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

args = parser.parse_args()
inPath, outPath = args.input_path, args.output_path

spark = SparkSession \
    .builder \
    .appName("Spark job1") \
    .getOrCreate()

linesRDD=spark.sparkContext.textFile(inPath).cache()

action2TickerRDD=linesRDD.flatMap(f=lambda action: [action.split(",")])


action2TickerRDD=action2TickerRDD.map(f=lambda action: (str(action[1])+","+str(action[2])+","+str(action[9][:4]),action[5:]))
schema=action2TickerRDD.first()
action2TickerRDD=action2TickerRDD.filter(lambda line: line!=schema)

action2FT=action2TickerRDD.reduceByKey(lambda x,y:x if x[4]<y[4] else y)
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

yearRDD=action2var.join(action2min).join(action2max).join(action2avgV).sortByKey()
yearRDD=yearRDD.map(f=lambda action:(action[0].split(","),action[1]))
yearRDD=yearRDD.map(f=lambda action:("ticker: "+str(action[0][0])+", nome:"+str(action[0][1])+", anni:",
                                     "anno: "+str(action[0][2])+" var percentuale: "+str(int(action[1][0][0][0]))+
                                     "%,  min: "+str("%.2f" %float(action[1][0][0][1]))+", max: "+str("%.2f" %float(action[1][0][1]))+
                                     ", volume medio: "+str(int(action[1][1]))))
yearRDD=yearRDD.groupByKey().mapValues(list)
yearRDD.saveAsTextFile(outPath)

