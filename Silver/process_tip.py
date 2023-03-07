import os
import re
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as f
from pyspark.ml.feature import StopWordsRemover

load_dotenv()
awsAccessKey = os.environ.get('AWS_ACCESS_KEY')
awsAccessSecret = os.environ.get('AWS_ACCESS_SECRET')
awsS3Directory = os.environ.get('AWS_S3_DIRECTORY')

spark = SparkSession.builder.appName("Business Data Processing").getOrCreate()

spark.sparkContext\
     ._jsc.hadoopConfiguration().set("fs.s3a.access.key", awsAccessKey)
spark.sparkContext\
     ._jsc.hadoopConfiguration().set("fs.s3a.secret.key", awsAccessSecret)
spark.sparkContext\
      ._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
spark.sparkContext\
      ._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

df = spark.read.option("inferSchema","true").parquet(awsS3Directory + "/Bronze/tip_data")

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

df = splitTimeStamp(df)
df = removeTipStopWords(df)

df.write.parquet(awsS3Directory + "/Silver/tip_data")

