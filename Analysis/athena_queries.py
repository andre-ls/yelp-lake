import os
from dotenv import load_dotenv
from pyathena import connect
import pandas as pd

load_dotenv()
awsAccessKey = os.environ.get('AWS_ACCESS_KEY')
awsAccessSecret = os.environ.get('AWS_ACCESS_SECRET')
awsS3Directory = os.environ.get('AWS_S3_DIRECTORY')

conn = connect(aws_access_key_id=awsAccessKey,
                 aws_secret_access_key=awsAccessSecret,
                 s3_staging_dir="s3://yelp-lake/Athena_Results",
                 region_name="sa-east-1"
                )

def getBusinessNames(search_query):

    query = """
        SELECT b.business_id,
        b.name || ' - ' || b.address || ', ' || b.city || ', ' || b.state AS query_results
        FROM yelp_data.business_data b
        WHERE b.name LIKE '%{search_query}%'
    """

    return pd.read_sql_query(query.format(search_query = search_query),conn)

def getBusinessData(business_id):

    query = """
        SELECT b.business_id, 
                b.name, 
                b.address, 
                b.city, 
                b.state, 
                b.latitude, 
                b.longitude, 
                b.categories, 
                b.attributes_list
        FROM yelp_data.business_data b
        WHERE b.business_id = '{business_id}'
    """

    return pd.read_sql_query(query.format(business_id = business_id),conn)

def getReviewData(business_id):

    query = """
        SELECT r.avg_stars,
                r.filtered_text
        FROM yelp_data.business_data b
        INNER JOIN yelp_data.reviews_view r ON b.business_id = r.business_id
        WHERE b.business_id = '{business_id}'
    """

    return pd.read_sql_query(query.format(business_id = business_id),conn)

def getCheckinData(business_id):

    query = """
        SELECT c.date,
                c.count
        FROM yelp_data.business_data b
        INNER JOIN yelp_data.checkins_view c ON b.business_id = c.business_id
        WHERE b.business_id = '{business_id}'
    """

    return pd.read_sql_query(query.format(business_id = business_id),conn)

def getTipData(business_id):

    query = """
        SELECT t.avg_compliment_count,
                t.filtered_text
        FROM yelp_data.business_data b
        INNER JOIN yelp_data.tips_view t ON b.business_id = t.business_id
        WHERE b.business_id = '{business_id}'
    """

    return pd.read_sql_query(query.format(business_id = business_id),conn)

def getFrequentCustomersData(business_id):

    query = """
        SELECT u.name, 
                u.cool, 
                u.funny, 
                u.review_count, 
                u.yelping_since,
                f.count
        FROM yelp_data.business_data b
        INNER JOIN yelp_data.frequent_customers_view f ON b.business_id = f.business_id
        INNER JOIN yelp_data.user_data u ON u.user_id = f.user_id
        WHERE b.business_id = '{business_id}'
    """

    return pd.read_sql_query(query.format(business_id = business_id),conn)

