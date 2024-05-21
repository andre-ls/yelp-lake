import os
import re
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from Bronze.ingestion import ingestData
from Silver import processBusinessData, processCheckinData, processReviewData, processTipData, processUserData
from Gold import checkins_view, frequent_customers_view, reviews_view, tips_view

load_dotenv()
bucketUrl = os.environ.get("BUCKET_URL")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Yelp ETL").getOrCreate()

    # Bronze Jobs
    #ingestData(spark, bucketUrl)

    # Silver Jobs
    #processBusinessData.ProcessBusinessData(spark, bucketUrl + "/Bronze/business_data", bucketUrl + "/Silver/business_data").run()
    #processCheckinData.ProcessCheckinData(spark, bucketUrl + "/Bronze/checkin_data", bucketUrl + "/Silver/checkin_data").run()
    #processReviewData.ProcessReviewData(spark, bucketUrl + "/Bronze/review_data", bucketUrl + "/Silver/review_data").run()
    #processTipData.ProcessTipData(spark, bucketUrl + "/Bronze/tip_data", bucketUrl + "/Silver/tip_data").run()
    processUserData.ProcessUserData(spark, bucketUrl + "/Bronze/user_data", bucketUrl + "/Silver/user_data").run()

    # Gold Jobs
    #checkins_view.createView(spark)
    #frequent_customers_view.createView(spark)
    #reviews_view.createView(spark)
    #tips_view.createView(spark)
