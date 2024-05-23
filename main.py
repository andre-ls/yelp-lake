from pyspark.sql import SparkSession
from Bronze.ingestion import ingestData
from Silver import processBusinessData, processCheckinData, processReviewData, processTipData, processUserData
from Gold import createCheckinsView, createFrequentCustomersView, createReviewsView, createTipsView

bucketUrl = "gs://yelp-lake"

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Yelp ETL").getOrCreate()

    # Bronze Jobs
    ingestData(spark, bucketUrl)

    # Silver Jobs
    processBusinessData.ProcessBusinessData(spark, bucketUrl + "/Bronze/business_data", bucketUrl + "/Silver/business_data").run()
    processCheckinData.ProcessCheckinData(spark, bucketUrl + "/Bronze/checkin_data", bucketUrl + "/Silver/checkin_data").run()
    processReviewData.ProcessReviewData(spark, bucketUrl + "/Bronze/review_data", bucketUrl + "/Silver/review_data").run()
    processTipData.ProcessTipData(spark, bucketUrl + "/Bronze/tip_data", bucketUrl + "/Silver/tip_data").run()
    processUserData.ProcessUserData(spark, bucketUrl + "/Bronze/user_data", bucketUrl + "/Silver/user_data").run()

    # Gold Jobs
    createCheckinsView.CreateCheckinsView(spark, bucketUrl + "/Silver/checkin_data", bucketUrl + "/Gold/checkins_view").run()
    createFrequentCustomersView.CreateFrequentCustomersView(spark, bucketUrl + "/Silver/tip_data", bucketUrl + "/Gold/frequent_customers_view").run()
    createReviewsView.CreateReviewsView(spark, bucketUrl + "/Silver/review_data", bucketUrl + "/Gold/reviews_view").run()
    createTipsView.CreateTipsView(spark, bucketUrl + "/Silver/tip_data", bucketUrl + "/Gold/tips_view").run()
