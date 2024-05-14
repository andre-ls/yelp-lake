import os
import re
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from Bronze.ingestion import ingestData
from Silver import process_business, process_checkin, process_review, process_tip, process_user

load_dotenv()
spark = SparkSession.builder.appName("Business Data Load").getOrCreate()

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Yelp ETL").getOrCreate()
    process_business.process(spark)
    process_checkin.process(spark)
    process_review.process(spark)
    process_tip.process(spark)
    process_user.process(spark)
