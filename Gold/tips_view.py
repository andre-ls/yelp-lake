import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as f

load_dotenv()
bucketUrl = os.environ.get('BUCKET_URL')

def createView(spark):
    df = spark.read.option("inferSchema","true").parquet(bucketUrl + "/Silver/tip_data")

    df = df.groupBy("business_id")\
           .agg(f.avg("compliment_count").alias("avg_compliment_count"),\
                f.flatten(f.collect_list("filtered_text")).alias("filtered_text")\
            )

    df.write.mode('overwrite').parquet(bucketUrl + "/Gold/tips_view")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Tips Gold View").getOrCreate()
    createView(spark)
