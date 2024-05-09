import os
import re
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from Bronze.ingestion import ingestData

load_dotenv()
spark = SparkSession.builder.appName("Business Data Load").getOrCreate()

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Yelp ETL").getOrCreate()
    ingestData(spark)
