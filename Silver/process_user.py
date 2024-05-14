import os
import re
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as f
from pyspark.ml.feature import StopWordsRemover

load_dotenv()
bucketUrl = os.environ.get('BUCKET_URL')

def splitFriends(df):
    df = df.withColumn("friends",f.split(f.col("friends"), ", "))
    return df

def process(spark):
    df = spark.read.option("inferSchema","true").parquet(bucketUrl + "/Bronze/user_data")
    df = splitFriends(df)
    df.write.parquet(bucketUrl + "/Silver/user_data")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("User Data Processing").getOrCreate()
    process(spark)
