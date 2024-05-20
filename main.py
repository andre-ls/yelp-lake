import os
import re
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from Bronze.ingestion import ingestData
from Silver import process_business, process_checkin, process_review, process_tip, process_user
from Gold import checkins_view, frequent_customers_view, reviews_view, tips_view

load_dotenv()
bucketUrl = os.environ.get("BUCKET_URL")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Yelp ETL").getOrCreate()

    # Bronze Jobs
    ingestData(spark, bucketUrl)

    # Silver Jobs
    process_business.process(spark)
    process_checkin.process(spark)
    process_review.process(spark)
    process_tip.process(spark)
    process_user.process(spark)

    # Gold Jobs
    checkins_view.createView(spark)
    frequent_customers_view.createView(spark)
    reviews_view.createView(spark)
    tips_view.createView(spark)
