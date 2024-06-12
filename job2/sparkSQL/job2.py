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

w=Window.partitionBy("industry","year")
maxT=industryDF.withColumn("mt",F.max("var").over(w)).where(F.col("var")==F.col("mt")).drop("mt").select("sector", "industry",F.col("ticker").alias("mt"),"year",F.col("var").alias("mv"))
maxT=maxT.withColumn("mt", F.col("mt").cast(StringType()).alias("mt"))
maxT=maxT.withColumn("mv", F.col("mv").cast(StringType()).alias("mv"))
maxT=maxT.withColumn("maxVar", F.concat_ws(" ",maxT.mv,maxT.mt))
industryDF=industryDF.join(maxT, ["sector", "industry","year"],"left").drop("close","date",("mt"),("mv"))

industryDF.show()



industryDF=industryDF.groupBy("sector","industry","year","ticker").agg(F.max(F.col("maxVar")).alias("maxVar"),
                                                                       F.max("var").alias("var"),
                                                                       F.max("volume").alias("volume")).orderBy("year")

industryDF=industryDF.groupBy("sector","industry","year").agg(F.max("maxVar").alias("maxVar"),
                                                              F.collect_list("ticker").alias("ticker"),
                                                              F.collect_list("var").alias("var"),
                                                              F.collect_list("volume").alias("volume")).orderBy("maxVar")

industryDF=industryDF.groupBy("sector","industry").agg(F.collect_list("year").alias("year"),
                                                       F.collect_list("maxVar").alias("maxVar"),
                                                       F.collect_list("ticker").alias("ticker"),
                                                       F.collect_list("var").alias("var"),
                                                       F.collect_list("volume").alias("volume")).orderBy("sector")

industryDF=industryDF.withColumn("year", F.col("year").cast(StringType()))
industryDF=industryDF.withColumn("ticker", F.col("ticker").cast(StringType()))
industryDF=industryDF.withColumn("var", F.col("var").cast(StringType()))
industryDF=industryDF.withColumn("volume", F.col("volume").cast(StringType()))


industryDF.write.csv(outPath)