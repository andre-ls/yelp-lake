import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as f

load_dotenv()
bucketUrl = os.environ.get('BUCKET_URL')

def createView(spark):
    df = spark.read.option("inferSchema","true").parquet(bucketUrl + "/Silver/tip_data")
    df = df.groupBy(["business_id","user_id"]).count()
    df.write.mode('overwrite').parquet(bucketUrl + "/Gold/frequent_customers_view")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Frequent Customers Gold View").getOrCreate()
    createView(spark)
