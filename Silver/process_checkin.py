import os
import re
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as f

load_dotenv()
awsAccessKey = os.environ.get('AWS_ACCESS_KEY')
awsAccessSecret = os.environ.get('AWS_ACCESS_SECRET')
awsS3Directory = os.environ.get('AWS_S3_DIRECTORY')

spark = SparkSession.builder.appName("Business Data Processing").getOrCreate()

spark.sparkContext\
     ._jsc.hadoopConfiguration().set("fs.s3a.access.key", awsAccessKey)
spark.sparkContext\
     ._jsc.hadoopConfiguration().set("fs.s3a.secret.key", awsAccessSecret)
spark.sparkContext\
      ._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
spark.sparkContext\
      ._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

df = spark.read.option("inferSchema","true").parquet(awsS3Directory + "/Bronze/checkin_data")

def explodeDates(df):
    df = df.withColumn("date",f.split(f.col("date"),", "))
    df = df.withColumn("timestamp",f.explode("date"))
    return df

def splitTimeStamp(df):
    df = df.withColumn("date",f.split(f.col("timestamp"), " ").getItem(0))
    df = df.withColumn("hour",f.split(f.col("timestamp"), " ").getItem(1))
    df = df.drop(f.col("timestamp"))
    return df

df = explodeDates(df)
df = splitTimeStamp(df)

df.write.parquet(awsS3Directory + "/Silver/checkin_data")
