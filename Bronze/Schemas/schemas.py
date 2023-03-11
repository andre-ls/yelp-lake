from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,TimestampType,FloatType,ArrayType,DoubleType

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
        StructField("attributes",StructType([
                StructField("AcceptsInsurance",StringType(),True),
                StructField("AgesAllowed",StringType(),True),
                StructField("Alcohol",StringType(),True),
                StructField("Ambience",StringType(),True),
                StructField("BYOB",StringType(),True),
                StructField("BYOBCorkage",StringType(),True),
                StructField("BestNights",StringType(),True),
                StructField("BikeParking",StringType(),True),
                StructField("BusinessAcceptsBitcoin",StringType(),True),
                StructField("BusinessAcceptsCreditCards",StringType(),True),
                StructField("BusinessParking",StringType(),True),
                StructField("ByAppointmentOnly",StringType(),True),
                StructField("Caters",StringType(),True),
                StructField("CoatCheck",StringType(),True),
                StructField("Corkage",StringType(),True),
                StructField("DietaryRestrictions",StringType(),True),
                StructField("DogsAllowed",StringType(),True),
                StructField("DriveThru",StringType(),True),
                StructField("GoodForDancing",StringType(),True),
                StructField("GoodForKids",StringType(),True),
                StructField("GoodForMeal",StringType(),True),
                StructField("HairSpecializesIn",StringType(),True),
                StructField("HappyHour",StringType(),True),
                StructField("HasTV",StringType(),True),
                StructField("Music",StringType(),True),
                StructField("NoiseLevel",StringType(),True),
                StructField("Open24Hours",StringType(),True),
                StructField("OutdoorSeating",StringType(),True),
                StructField("RestaurantsAttire",StringType(),True),
                StructField("RestaurantsCounterService",StringType(),True),
                StructField("RestaurantsDelivery",StringType(),True),
                StructField("RestaurantsGoodsForGroups",StringType(),True),
                StructField("RestaurantsPriceRange2",StringType(),True),
                StructField("RestaurantsReservations",StringType(),True),
                StructField("RestaurantsTableService",StringType(),True),
                StructField("RestaurantsTakeOut",StringType(),True),
                StructField("Smoking",StringType(),True),
                StructField("WheelchairAccesible",StringType(),True),
                StructField("Wifi",StringType(),True)
            ])),
        StructField("categories",StringType(),False),
        StructField("hours",StructType([
                StructField("Sunday",StringType(),True),
                StructField("Monday",StringType(),True),
                StructField("Tuesday",StringType(),True),
                StructField("Wednesday",StringType(),True),
                StructField("Thursday",StringType(),True),
                StructField("Friday",StringType(),True),
                StructField("Saturday",StringType(),True)
            ]))
    ])

checkinSchema = StructType([
        StructField("business_id",StringType(),False),
        StructField("date",StringType(),False),
    ])

reviewSchema = StructType([
        StructField("review_id",StringType(),False),
        StructField("user_id",StringType(),False),
        StructField("business_id",StringType(),False),
        StructField("stars",FloatType(),False),
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
        StructField("average_stars",DoubleType(),False),
        StructField("compliment_cool",IntegerType(),False),
        StructField("compliment_cute",IntegerType(),False),
        StructField("compliment_funny",IntegerType(),False),
        StructField("compliment_hot",IntegerType(),False),
        StructField("compliment_list",IntegerType(),False),
        StructField("compliment_more",IntegerType(),False),
        StructField("compliment_note",IntegerType(),False),
        StructField("compliment_photos",IntegerType(),False),
        StructField("compliment_plain",IntegerType(),False),
        StructField("compliment_profile",IntegerType(),False),
        StructField("compliment_writer",IntegerType(),False),
        StructField("cool",IntegerType(),False),
        StructField("elite",StringType(),False),
        StructField("fans",IntegerType(),False),
        StructField("friends",StringType(),False),
        StructField("funny",IntegerType(),False),
        StructField("name",StringType(),False),
        StructField("review_count",IntegerType(),False),
        StructField("useful",IntegerType(),False),
        StructField("user_id",StringType(),False),
        StructField("yelping_since",TimestampType(),False)
    ])
