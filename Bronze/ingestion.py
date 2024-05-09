import os
from dotenv import load_dotenv
from Bronze.Schemas.schemas import *

load_dotenv()
bucketUrl = os.environ.get('BUCKET_URL')

def ingest(spark, schema, rawDirectory, bronzeDirectory):
    df = spark.read.schema(schema).json(bucketUrl + rawDirectory)
    df.write.parquet(bucketUrl + bronzeDirectory)

def ingestBusinessData(spark):
    ingest(spark, businessSchema, "/Raw/yelp_academic_dataset_business.json", "/Bronze/business_data")

def ingestCheckIn(spark):
    ingest(spark, checkinSchema, "/Raw/yelp_academic_dataset_checkin.json", "/Bronze/checkin_data")

def ingestReview(spark):
    ingest(spark, reviewSchema, "/Raw/yelp_academic_dataset_review.json", "/Bronze/review_data")

def ingestTip(spark):
    ingest(spark, tipSchema, "/Raw/yelp_academic_dataset_tip.json", "/Bronze/tip_data")

def ingestUser(spark):
    ingest(spark, userSchema, "/Raw/yelp_academic_dataset_user.json", "/Bronze/user_data")

def ingestData(spark):
    ingestBusinessData(spark)
    ingestCheckIn(spark)
    ingestReview(spark)
    ingestTip(spark)
    ingestUser(spark)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Data Ingestion").getOrCreate()
    ingestData(spark)
