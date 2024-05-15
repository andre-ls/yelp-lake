import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as f

load_dotenv()
bucketUrl = os.environ.get('BUCKET_URL')

def createView(spark):
    df = spark.read.option("inferSchema","true").parquet(bucketUrl + "/Silver/review_data")

    df = df.groupBy("business_id")\
           .agg(f.avg("stars").alias("avg_stars"),\
                f.count("*").alias("number_reviews"),\
                f.avg("useful").alias("avg_useful"),\
                f.avg("funny").alias("avg_funny"),\
                f.avg("cool").alias("avg_cool"),\
                f.flatten(f.collect_list("filtered_text")).alias("filtered_text")\
            )

    df.write.mode('overwrite').parquet(bucketUrl + "/Gold/reviews_view")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Reviews Gold View").getOrCreate()
    createView(spark)
