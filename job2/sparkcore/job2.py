#!/usr/bin/env python3
"""job2.py"""

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
    .appName("Spark job2") \
    .getOrCreate()

linesRDD=spark.sparkContext.textFile(inPath).cache()

industryRDD=linesRDD.map(f=lambda action: action.split(","))


schema=industryRDD.first()

industryDF=spark.createDataFrame(data=industryRDD, schema=schema)

industryDF=industryDF.withColumn("year", industryDF["date"].substr(0,4))

industryDF=industryDF.drop("low","high","", "name")

w=Window.partitionBy("ticker")
fs=industryDF.withColumn("fs",F.min("date").over(w)).where(F.col("date")==F.col("fs")).drop("fs").select("sector", "industry","ticker","year",F.col("close").alias("fs"))
ls=industryDF.withColumn("ls",F.max("date").over(w)).where(F.col("date")==F.col("ls")).drop("ls").select("sector", "industry","ticker","year",F.col("close").alias("ls"))

industryDF=industryDF.join(fs, ["sector", "industry","ticker","year"])
industryDF=industryDF.join(ls, ["sector", "industry","ticker","year"])
industryDF=industryDF.withColumn("var", (industryDF.ls-industryDF.fs)/industryDF.fs*100).drop("fs","ls")

w=Window.partitionBy("industry","year")
maxT=industryDF.withColumn("mt",F.max("var").over(w)).where(F.col("var")==F.col("mt")).drop("mt").select("sector", "industry",F.col("ticker").alias("mt"),"year",F.col("var").alias("mv"))
maxT=maxT.withColumn("maxVar", maxT.mv+maxT.mt)
industryDF=industryDF.join(maxT, ["sector", "industry","year"]).drop("close","date")
industryDF=industryDF.groupBy("sector","industry","year","ticker").agg(F.max("maxVar").alias("maxVar"), F.max("var").alias("var"), F.max("volume").alias("volume"))
industryDF=industryDF.groupBy("sector","industry","year").agg(F.max("maxVar").alias("maxVar"),F.collect_list("ticker").alias("ticker"),F.collect_list("var").alias("var"),F.collect_list("volume").alias("volume"))
industryDF=industryDF.orderBy("maxVar")

industryDF=industryDF.groupBy("sector","industry").agg(F.max("maxVar").alias("maxVar"),F.collect_list("year").alias("year"), F.collect_list("ticker").alias("ticker"),F.collect_list("var").alias("var"),F.collect_list("volume").alias("volume"))


data=industryDF.collect()
final={}
print(data)
for row in data:
    if row["sector"] in final.keys():
        final[row["sector"]].append([row["industry"],row["year"],row["maxVar"],row["ticker"],row["var"],row["volume"]])
    else:
        final[row["sector"]]=[row["industry"],row["year"],row["maxVar"],row["ticker"],row["var"],row["volume"]]

s=[]

for industry in final.keys():
    s.append([f"sector: {industry}"])
    s.append([f"industry: {final[industry][0]}"])
    for i in range(len(final[industry][1])):
        s.append([f"\tanno: {final[industry][1][i]}, max var percentuale: {final[industry][2]}%"])
        for j in range(len(final[industry][3][i])):
            s.append([f"\tticker: {final[industry][3][i][j]}, var percentuale: {final[industry][4][i][j]}%, max volume: {final[industry][5][i][j]}"])
    s.append(["\n"])

spark.createDataFrame(spark.sparkContext.parallelize(s)).coalesce(1).write.format("text").save(outPath)