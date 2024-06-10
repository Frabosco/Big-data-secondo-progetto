#!/usr/bin/env python3
"""job1.py"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import Window
import pyspark.sql.functions as F

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

action2TickerRDD=linesRDD.map(f=lambda action: action.split(","))


schema=action2TickerRDD.first()

tickerDF=spark.createDataFrame(data=action2TickerRDD, schema=schema)

tickerDF=tickerDF.drop("sector","industry","")

tickerDF=tickerDF.withColumn("year", tickerDF["date"].substr(0,4))

w=Window.partitionBy("year")
fs=tickerDF.withColumn("fs",F.min("date").over(w)).where(F.col("date")==F.col("fs")).drop("fs").select("ticker","name","year",F.col("close").alias("fs"))
ls=tickerDF.withColumn("ls",F.max("date").over(w)).where(F.col("date")==F.col("ls")).drop("ls").select("ticker","name","year",F.col("close").alias("ls"))

tickerDF=tickerDF.groupBy("ticker","name","year").agg(F.min("low").alias("minP"), F.max("high").alias("maxP"),F.avg("volume").alias("avgV")).orderBy("year")
tickerDF=tickerDF.join(fs, ["ticker","name","year"])
tickerDF=tickerDF.join(ls, ["ticker","name","year"])

tickerDF=tickerDF.withColumn("var", (tickerDF.ls-tickerDF.fs)/tickerDF.fs*100).drop("fs","ls")
data=tickerDF.collect()
final={}
for row in data:
    if (row["ticker"],row["name"]) in final.keys():
        final[(row["ticker"],row["name"]) ].append([row["year"],row["var"],row["minP"],row["maxP"],row["avgV"]])
    else:
        final[(row["ticker"],row["name"])]=[[row["year"],row["var"],row["minP"],row["maxP"],row["avgV"]]]

s=[]

for key in final.keys():
    s.append([f"ticker: {key[0]}, nome: {key[1]}, anni:"])

    for year in final[key]:
        s.append([f"\tanno: {year[0]}, var percentuale: {year[1]}%,  min: {year[2]}, max: {year[3]}, volume medio: {year[4]}"])

    s.append(["\n"])

spark.createDataFrame(spark.sparkContext.parallelize(s)).coalesce(1).write.format("text").save(outPath)




