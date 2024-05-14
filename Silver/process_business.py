import os
import re
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as f

load_dotenv()
bucketURL = os.environ.get('BUCKET_URL')

def convertBooleanAttributesToList(df):
    df_attributes = df.select("attributes.*")
    non_null_cols = [f.when(f.col("attributes." + c) == "True", f.lit(c)) for c in df_attributes.columns]
    df = df.withColumn("attributes_list", f.array(*non_null_cols))\
           .withColumn("attributes_list", f.expr("array_join(attributes_list, ',')"))

    df = df.drop(f.col("attributes"))
    return df

def process(spark):
    df = spark.read.option("inferSchema","true").parquet(bucketURL + "/Bronze/business_data")
    df = convertBooleanAttributesToList(df)
    df.write.parquet(bucketURL + "/Silver/business_data")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Business Data Processing").getOrCreate()
    process(spark)
