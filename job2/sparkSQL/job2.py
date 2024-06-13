#!/usr/bin/env python3
"""job2.py"""

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
    .appName("Spark job2") \
    .getOrCreate()

linesRDD=spark.sparkContext.textFile(inPath).cache()

industryRDD=linesRDD.map(f=lambda action: action.split(","))


schema=industryRDD.first()
industryRDD=industryRDD.filter(lambda line: line!=schema)

industryDF=spark.createDataFrame(data=industryRDD, schema=schema)


industryDF=industryDF.withColumn("year", industryDF["date"].substr(0,4))

industryDF=industryDF.drop("low","high","", "name")

w=Window.partitionBy("ticker","year")
fs=industryDF.withColumn("fs",F.min("date").over(w)).where(F.col("date")==F.col("fs")).drop("fs").select("sector", "industry","ticker","year",F.col("close").alias("fs"))
ls=industryDF.withColumn("ls",F.max("date").over(w)).where(F.col("date")==F.col("ls")).drop("ls").select("sector", "industry","ticker","year",F.col("close").alias("ls"))
industryDF=industryDF.join(fs, ["sector", "industry","ticker","year"],"left")
industryDF=industryDF.join(ls, ["sector", "industry","ticker","year"],"left")
industryDF=industryDF.withColumn("var", (industryDF.ls-industryDF.fs)/industryDF.fs*100).drop("fs","ls")

quot=industryDF.groupBy("sector","industry","year").agg(F.sum("volume").cast(StringType()).alias("quot"))

w=Window.partitionBy("industry","year")
maxTVar=industryDF.withColumn("mt",F.max("var").over(w)).where(F.col("var")==F.col("mt")).drop("mt").select("sector", "industry",F.col("ticker").alias("mt"),"year",F.col("var").alias("mv"))
maxTVar=maxTVar.withColumn("mt", F.col("mt").cast(StringType()).alias("mt"))
maxTVar=maxTVar.withColumn("mv", F.col("mv").cast(StringType()).alias("mv"))
maxTVar=maxTVar.withColumn("maxVar", F.concat_ws("-",maxTVar.mv,maxTVar.mt)).drop("mt","mv")

maxTVol=industryDF.withColumn("mt",F.max("volume").over(w)).where(F.col("volume")==F.col("mt")).drop("mt").select("sector", "industry",F.col("ticker").alias("mt"),"year",F.col("volume").alias("mv"))
maxTVol=maxTVol.withColumn("mt", F.col("mt").cast(StringType()).alias("mt"))
maxTVol=maxTVol.withColumn("mv", F.col("mv").cast(StringType()).alias("mv"))
maxTVol=maxTVol.withColumn("maxVol", F.concat_ws("-",maxTVol.mv,maxTVol.mt)).drop("mt","mv")
tickerDF=maxTVar.join(maxTVol, ["sector", "industry","year"]).groupBy("sector","industry","year").agg(F.max("maxVar").alias("maxVar"),
                                                                                                     F.max("maxVol").alias("maxVol"))
tickerDF=tickerDF.join(quot, ["sector", "industry","year"])

yearDF=tickerDF.groupBy("sector","industry").agg(F.collect_list(F.concat_ws(", ",tickerDF.year,tickerDF.quot,tickerDF.maxVar,
                                                                            tickerDF.maxVol)).alias("year")).orderBy("sector")

industryDF=yearDF.groupBy("sector").agg(F.collect_list(F.concat_ws(", ",yearDF.industry,yearDF.year)).alias("industry"))
industryDF=industryDF.withColumn("industry", F.col("industry").cast(StringType()).alias("industry"))

industryDF.write.csv(outPath)