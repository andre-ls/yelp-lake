import os
import re
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as f

load_dotenv()
bucketUrl = os.environ.get('BUCKET_URL')

def explodeDates(df):
    df = df.withColumn("date",f.split(f.col("date"),", "))
    df = df.withColumn("timestamp",f.explode("date"))
    return df

def splitTimeStamp(df):
    df = df.withColumn("date",f.split(f.col("timestamp"), " ").getItem(0))
    df = df.withColumn("hour",f.split(f.col("timestamp"), " ").getItem(1))
    df = df.drop(f.col("timestamp"))
    return df

def process(spark):
    df = spark.read.option("inferSchema","true").parquet(bucketUrl + "/Bronze/checkin_data")
    df = explodeDates(df)
    df = splitTimeStamp(df)
    df.write.parquet(bucketUrl + "/Silver/checkin_data")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Checkin Data Processing").getOrCreate()
    process(spark)
