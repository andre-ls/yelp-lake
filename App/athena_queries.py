from pyathena import connect
import pandas as pd
import streamlit as st

awsAccessKey = st.secrets['AWS_ACCESS_KEY']
awsAccessSecret = st.secrets['AWS_ACCESS_SECRET']
awsS3Directory = st.secrets['AWS_S3_DIRECTORY']

conn = connect(aws_access_key_id=awsAccessKey,
                 aws_secret_access_key=awsAccessSecret,
                 s3_staging_dir="s3://yelp-lake/Athena_Results",
                 region_name="sa-east-1"
                )

def getRandomBusinessId():

    query = """
        SELECT b.business_id
        FROM yelp_data.business_data b
        ORDER BY RAND()
        LIMIT 1
    """
    return pd.read_sql_query(query,conn)["business_id"][0]

@st.cache_data
def getBusinessNames():

    query = """
        SELECT b.business_id,
        b.name || ' - ' || b.address || ', ' || b.city || ', ' || b.state AS query_results
        FROM yelp_data.business_data b
    """
        #WHERE b.name LIKE '%{search_query}%'

    return pd.read_sql_query(query,conn)

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
                r.number_reviews,
                r.filtered_text
        FROM yelp_data.business_data b
        INNER JOIN yelp_data.reviews_view r ON b.business_id = r.business_id
        WHERE b.business_id = '{business_id}'
    """

    return pd.read_sql_query(query.format(business_id = business_id),conn)

def getCheckinData(business_id):

    #query = """
    #    SELECT c.date,
    #            c.count
    #    FROM yelp_data.business_data b
    #    INNER JOIN yelp_data.checkins_view c ON b.business_id = c.business_id
    #    WHERE b.business_id = '{business_id}'
    #"""

    #query = """
    #SELECT SUBSTRING(c.date, 1, 7) as month,
    #            sum(c.count) as counts
    #    FROM yelp_data.checkins_view c 
    #    WHERE c.business_id = '{business_id}'
    #    GROUP BY SUBSTRING(c.date, 1, 7)
    #"""

    query = """
    SELECT c.date as date,
        c.count as counts
        FROM yelp_data.checkins_view c 
        WHERE c.business_id = '{business_id}'
        ORDER BY c.date DESC
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
        SELECT u.name as Name, 
                u.cool, 
                u.funny, 
                u.review_count, 
                u.yelping_since,
                f.count
        FROM yelp_data.business_data b
        INNER JOIN yelp_data.frequent_customers_view f ON b.business_id = f.business_id
        INNER JOIN yelp_data.user_data u ON u.user_id = f.user_id
        WHERE b.business_id = '{business_id}'
        ORDER BY f.count DESC
    """

    return pd.read_sql_query(query.format(business_id = business_id),conn)

def getReviewsDistribution(business_id):
    query = """
    SELECT DISTINCT r1.stars, COALESCE(g.count,0) AS count 
    FROM yelp_data.review_data r1
    LEFT JOIN (
        SELECT r2.stars,count(r2.stars) as count
        FROM yelp_data.review_data r2
        WHERE r2.business_id = '{business_id}'
        GROUP BY stars) g 
    ON r1.stars = g.stars
    GROUP BY r1.stars, g.count
    """

    return pd.read_sql_query(query.format(business_id = business_id),conn)

