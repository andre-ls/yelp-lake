import os
import re
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import StringType

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

df = spark.read.option("inferSchema","true").parquet(awsS3Directory + "/Bronze/business_data")

def convertBooleanAttributesToList(df):
    df_attributes = df.select("attributes.*")
    non_null_cols = [f.when(f.col("attributes." + c) == "True", f.lit(c)) for c in df_attributes.columns]
    df = df.withColumn("attributes_list", f.array(*non_null_cols))\
           .withColumn("attributes_list", f.expr("array_join(attributes_list, ',')"))

    df = df.drop(f.col("attributes"))
    return df

df = convertBooleanAttributesToList(df)

df.write.parquet(awsS3Directory + "/Silver/business_data")

