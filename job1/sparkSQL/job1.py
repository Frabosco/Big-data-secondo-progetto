#!/usr/bin/env python3
"""job1.py"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.types import StringType
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
action2TickerRDD=action2TickerRDD.filter(lambda line: line!=schema)

tickerDF=spark.createDataFrame(data=action2TickerRDD, schema=schema)

tickerDF=tickerDF.drop("sector","industry","")

tickerDF=tickerDF.withColumn("year", tickerDF["date"].substr(0,4))

w=Window.partitionBy("year")
fs=tickerDF.withColumn("fs",F.min("date").over(w)).where(F.col("date")==F.col("fs")).drop("fs").select("ticker","name","year",F.col("close").alias("fs"))
ls=tickerDF.withColumn("ls",F.max("date").over(w)).where(F.col("date")==F.col("ls")).drop("ls").select("ticker","name","year",F.col("close").alias("ls"))

tickerDF=tickerDF.groupBy("ticker","name","year").agg(F.min("low").alias("minP"), 
                                                      F.max("high").alias("maxP"),
                                                      F.avg("volume").alias("avgV")).orderBy("year")
tickerDF=tickerDF.join(fs, ["ticker","name","year"],"left")
tickerDF=tickerDF.join(ls, ["ticker","name","year"],"left")

tickerDF=tickerDF.withColumn("var", (tickerDF.ls-tickerDF.fs)/tickerDF.fs*100).drop("fs","ls")


tickerDF=tickerDF.groupBy("ticker","name").agg(F.collect_list(F.concat_ws(", ",tickerDF.year,tickerDF.minP,tickerDF.maxP,
                                                                            tickerDF.avgV)).alias("year")).orderBy("ticker","year")

tickerDF=tickerDF.withColumn("year", F.col("year").cast(StringType()))

tickerDF.orderBy("ticker","year").write.csv(outPath)

