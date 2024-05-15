import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as f

load_dotenv()
bucketUrl = os.environ.get('BUCKET_URL')

def createView(spark):
    df = spark.read.option("inferSchema","true").parquet(bucketUrl + "/Silver/checkin_data")
    df = df.groupBy(["business_id","date"]).count()
    df.write.mode('overwrite').parquet(bucketUrl + "/Gold/checkins_view")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Checkins Gold View").getOrCreate()
    createView(spark)
