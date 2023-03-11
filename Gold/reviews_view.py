import os
import re
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as f
from pyspark.ml.feature import StopWordsRemover

load_dotenv()
awsAccessKey = os.environ.get('AWS_ACCESS_KEY')
awsAccessSecret = os.environ.get('AWS_ACCESS_SECRET')
awsS3Directory = os.environ.get('AWS_S3_DIRECTORY')

spark = SparkSession.builder.appName("Review and Tips Gold View").getOrCreate()

spark.sparkContext\
     ._jsc.hadoopConfiguration().set("fs.s3a.access.key", awsAccessKey)
spark.sparkContext\
     ._jsc.hadoopConfiguration().set("fs.s3a.secret.key", awsAccessSecret)
spark.sparkContext\
      ._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
spark.sparkContext\
      ._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

df = spark.read.option("inferSchema","true").parquet(awsS3Directory + "/Silver/review_data")

df = df.groupBy("business_id")\
       .agg(f.avg("stars").alias("avg_stars"),\
            f.avg("useful").alias("avg_useful"),\
            f.avg("funny").alias("avg_funny"),\
            f.avg("cool").alias("avg_cool"),\
            f.flatten(f.collect_list("filtered_text")).alias("filtered_text")\
        )

df.show(n=5,vertical=True,truncate=True)

#df.write.parquet(awsS3Directory + "/Gold/reviews_tips_view")

