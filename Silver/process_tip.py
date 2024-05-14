import os
import re
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as f
from pyspark.ml.feature import StopWordsRemover

load_dotenv()
bucketUrl = os.environ.get('BUCKET_URL')

def splitTimeStamp(df):
    df = df.withColumn("timestamp",f.col("date"))
    df = df.withColumn("date",f.split(f.col("timestamp"), " ").getItem(0))
    df = df.withColumn("hour",f.split(f.col("timestamp"), " ").getItem(1))
    df = df.drop(f.col("timestamp"))
    return df

def removeTipStopWords(df):
    df = removeTextPunctuation(df)
    df = df.withColumn("no_punc_text",f.split(f.col("no_punc_text")," "))
    remover = StopWordsRemover(inputCol="no_punc_text",outputCol="filtered_text")

    df = remover.transform(df)
    return df.drop("no_punc_text","text")

def removeTextPunctuation(df):
    regex = r',|\.|&|\\|\||-|_|  '
    df = df.withColumn("no_punc_text",f.regexp_replace(f.col("text"), regex, ''))
    return df

def process(spark):
    df = spark.read.option("inferSchema","true").parquet(bucketUrl + "/Bronze/tip_data")
    df = splitTimeStamp(df)
    df = removeTipStopWords(df)
    df.write.parquet(bucketUrl + "/Silver/tip_data")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Tip Data Processing").getOrCreate()
    process(spark)
