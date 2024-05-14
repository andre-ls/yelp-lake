import os
import re
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as f
from pyspark.ml.feature import StopWordsRemover

load_dotenv()
bucketUrl = os.environ.get('BUCKET_URL')

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

def process(spark):
    df = spark.read.option("inferSchema","true").parquet(bucketUrl + "/Bronze/review_data")
    df = removeReviewStopWords(df)
    df.write.mode("overwrite").parquet(bucketUrl + "/Silver/review_data")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Review Data Processing").getOrCreate()
    process(spark)

