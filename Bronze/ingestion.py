import os
from dotenv import load_dotenv
from Bronze.Schemas.schemas import *

load_dotenv()
bucketUrl = os.environ.get('BUCKET_URL')

class Ingestion:
    def __init__(self, spark, schema, bucketUrl, rawDirectory, bronzeDirectory):
        self.spark = spark
        self.schema = schema
        self.rawDirectory = bucketUrl + rawDirectory
        self.bronzeDirectory = bucketUrl + bronzeDirectory

    def run(self):
        df = spark.read.schema(self.schema).json(self.rawDirectory)
        df.write.mode('overwrite').parquet(self.bronzeDirectory)

def ingestData(spark, bucketUrl):
    Ingestion(spark, businessSchema, bucketUrl, "/Raw/yelp_academic_dataset_business.json", "/Bronze/business_data").run() #Business Data
    Ingestion(spark, checkinSchema, bucketUrl, "/Raw/yelp_academic_dataset_checkin.json", "/Bronze/checkin_data").run() #Checkin Data
    Ingestion(spark, reviewSchema, bucketUrl, "/Raw/yelp_academic_dataset_review.json", "/Bronze/review_data").run() #Review Data
    Ingestion(spark, tipSchema, bucketUrl, "/Raw/yelp_academic_dataset_tip.json", "/Bronze/tip_data").run() #Tip Data
    Ingestion(spark, userSchema, bucketUrl, "/Raw/yelp_academic_dataset_user.json", "/Bronze/user_data").run() #User Data

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Data Ingestion").getOrCreate()
    ingestData(spark)
