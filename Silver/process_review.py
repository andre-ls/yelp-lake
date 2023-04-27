import os
import re
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as f
from pyspark.ml.feature import StopWordsRemover

load_dotenv()
awsAccessKey = os.environ.get('AWS_ACCESS_KEY')
awsAccessSecret = os.environ.get('AWS_ACCESS_SECRET')
awsS3Directory = os.environ.get('AWS_S3_DIRECTORY')

spark = SparkSession.builder.appName("Review Data Processing").getOrCreate()

spark.sparkContext\
     ._jsc.hadoopConfiguration().set("fs.s3a.access.key", awsAccessKey)
spark.sparkContext\
     ._jsc.hadoopConfiguration().set("fs.s3a.secret.key", awsAccessSecret)
spark.sparkContext\
      ._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
spark.sparkContext\
      ._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

df = spark.read.option("inferSchema","true").parquet(awsS3Directory + "/Bronze/review_data")

def removeReviewStopWords(df):
    df = cleanText(df)
    remover = StopWordsRemover(inputCol="clean_text",outputCol="filtered_text")

    df = remover.transform(df)
    return df.drop("clean_text","text")

def cleanText(df):
    regex = r',|\.|&|-|_|\'.|\"|\?|\!'
    df = df.withColumn("clean_text",f.regexp_replace(f.col("text"), regex, ''))
    df = df.withColumn("clean_text",f.regexp_replace(f.col("clean_text"), r'[\n]', ' '))
    df = df.withColumn("clean_text",f.lower(f.col("clean_text")))
    df = df.withColumn("clean_text",f.split(f.col("clean_text")," "))
    df = df.withColumn("clean_text",f.filter(f.col("clean_text"),lambda x: x!=''))
    return df

def countWordsMap(df):
    counts = df.rdd.flatMap(lambda a: [(w,1) for w in a.filtered_text]).reduceByKey(lambda a,b: a+b).collect()
    print(counts)

df = removeReviewStopWords(df)

df.write.mode("overwrite").parquet(awsS3Directory + "/Silver/review_data")

