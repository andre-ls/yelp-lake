import pandas_gbq
import streamlit as st
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_info(
    {
        "type": st.secrets.type,
        "project_id": st.secrets.project_id,
        "private_key_id": st.secrets.private_key_id,
        "private_key": st.secrets.private_key,
        "client_email": st.secrets.client_email,
        "client_id": st.secrets.client_id,
        "auth_uri": st.secrets.auth_uri,
        "token_uri": st.secrets.token_uri,
        "auth_provider_x509_cert_url": st.secrets.auth_provider_x509_cert_url,
        "client_x509_cert_url": st.secrets.client_x509_cert_url
    },
)

def getRandomBusinessId():

    query = """
        SELECT b.business_id
        FROM yelp.business_data b
        ORDER BY RAND()
        LIMIT 1
    """
    return pandas_gbq.read_gbq(query, credentials = credentials)["business_id"][0]

@st.cache_data
def getBusinessNames():

    query = """
        SELECT b.business_id,
        b.name || ' - ' || b.address || ', ' || b.city || ', ' || b.state AS query_results
        FROM yelp.business_data b
    """
        #WHERE b.name LIKE '%{search_query}%'

    return pandas_gbq.read_gbq(query, credentials = credentials)

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
        FROM yelp.business_data b
        WHERE b.business_id = '{business_id}'
    """

    return pandas_gbq.read_gbq(query.format(business_id = business_id), credentials = credentials)

def getReviewData(business_id):

    query = """
        SELECT r.avg_stars,
                r.number_reviews,
                r.filtered_text
        FROM yelp.business_data b
        INNER JOIN yelp.reviews_view r ON b.business_id = r.business_id
        WHERE b.business_id = '{business_id}'
    """

    return pandas_gbq.read_gbq(query.format(business_id = business_id), credentials = credentials)

def getCheckinData(business_id):

    #query = """
    #    SELECT c.date,
    #            c.count
    #    FROM yelp.business_data b
    #    INNER JOIN yelp.checkins_view c ON b.business_id = c.business_id
    #    WHERE b.business_id = '{business_id}'
    #"""

    #query = """
    #SELECT SUBSTRING(c.date, 1, 7) as month,
    #            sum(c.count) as counts
    #    FROM yelp.checkins_view c 
    #    WHERE c.business_id = '{business_id}'
    #    GROUP BY SUBSTRING(c.date, 1, 7)
    #"""

    query = """
    SELECT c.date as date,
        c.count as counts
        FROM yelp.checkins_view c 
        WHERE c.business_id = '{business_id}'
        ORDER BY c.date DESC
    """

    return pandas_gbq.read_gbq(query.format(business_id = business_id), credentials = credentials)

def getTipData(business_id):

    query = """
        SELECT t.avg_compliment_count,
                t.filtered_text
        FROM yelp.business_data b
        INNER JOIN yelp.tips_view t ON b.business_id = t.business_id
        WHERE b.business_id = '{business_id}'
    """

    return pandas_gbq.read_gbq(query.format(business_id = business_id), credentials = credentials)

def getFrequentCustomersData(business_id):

    query = """
        SELECT u.name as Name, 
                u.cool, 
                u.funny, 
                u.review_count, 
                u.yelping_since,
                f.count
        FROM yelp.business_data b
        INNER JOIN yelp.frequent_customers_view f ON b.business_id = f.business_id
        INNER JOIN yelp.user_data u ON u.user_id = f.user_id
        WHERE b.business_id = '{business_id}'
        ORDER BY f.count DESC
    """

    return pandas_gbq.read_gbq(query.format(business_id = business_id), credentials = credentials)

def getReviewsDistribution(business_id):
    query = """
    SELECT DISTINCT r1.stars, COALESCE(g.count,0) AS count 
    FROM yelp.review_data r1
    LEFT JOIN (
        SELECT r2.stars,count(r2.stars) as count
        FROM yelp.review_data r2
        WHERE r2.business_id = '{business_id}'
        GROUP BY stars) g 
    ON r1.stars = g.stars
    GROUP BY r1.stars, g.count
    """

    return pandas_gbq.read_gbq(query.format(business_id = business_id), credentials = credentials)

