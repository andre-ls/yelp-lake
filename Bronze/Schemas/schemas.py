from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,TimestampType,FloatType,ArrayType

spark = SparkSession.builder.appName("Bronze Schemas").getOrCreate()

businessSchema = StructType([
        StructField("business_id",StringType(),False),
        StructField("name",StringType(),False),
        StructField("address",StringType(),False),
        StructField("city",StringType(),False),
        StructField("state",StringType(),False),
        StructField("postal_code",StringType(),False),
        StructField("latitude",FloatType(),False),
        StructField("longitude",FloatType(),False),
        StructField("stars",IntegerType(),False),
        StructField("review_count",IntegerType(),False),
        StructField("is_open",IntegerType(),False),
        StructField("attributes",ArrayType(StructType([
                StructField("ByAppointmentOnly",StringType(),True)
            ]))),
        StructField("categories",StringType(),False),
        StructField("hours",StringType(),True)
    ])

checkinSchema = StructType([
        StructField("business_id",StringType(),False),
        StructField("date",StringType(),False),
    ])

reviewSchema = StructType([
        StructField("review_id",StringType(),False),
        StructField("user_id",StringType(),False),
        StructField("business_id",StringType(),False),
        StructField("stars",IntegerType(),False),
        StructField("useful",IntegerType(),False),
        StructField("funny",IntegerType(),False),
        StructField("cool",IntegerType(),False),
        StructField("text",StringType(),False),
        StructField("date",TimestampType(),False)
    ])

tipSchema = StructType([
        StructField("user_id",StringType(),False),
        StructField("business_id",StringType(),False),
        StructField("text",StringType(),False),
        StructField("date",TimestampType(),False),
        StructField("compliment_count",IntegerType(),False)
    ])

userSchema = StructType([
        StructField("user_id",StringType(),False),
        StructField("name",StringType(),False),
        StructField("review_count",IntegerType(),False),
        StructField("yelping_since",TimestampType(),False),
        StructField("useful",IntegerType(),False),
        StructField("funny",IntegerType(),False),
        StructField("cool",IntegerType(),False),
        StructField("elite",StringType(),False),
        StructField("friends",StringType(),False)
    ])
