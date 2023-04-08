import numpy as np
import pandas as pd
import athena_queries as athena
import streamlit as st
from streamlit_searchbox import st_searchbox
from annotated_text import annotated_text

st.set_page_config(layout='wide')


# Introduction
column_image, column_title = st.columns([0.3,2.0])
with column_image:
    st.image('Images/yelp_logo.png',width=200)

with column_title:
    st.title('Yelp Business Data Insights')

# Search
def getBusinessNames(searchQuery):
    df = athena.getBusinessNames(searchQuery)
    return list(zip(df['query_results'], df['business_id']))

businessId = st_searchbox(
    search_function=getBusinessNames,
    placeholder="Enter Business Name...",
    label="Search Business",
    clear_on_submit=False,
    clearable=True,
)

# Data Visualization

businessData = athena.getBusinessData(businessId)
reviewData = athena.getReviewData(businessId)

def processTagList(tagList):
    return [(x,"") for x in tagList]

col1, col2, col3 = st.columns(3)

with col1:

    st.subheader(businessData["name"][0])

    subcol1, subcol2 = st.columns(2)

    with subcol1:
        st.metric(label="Average Rating", value=str(np.round(reviewData["avg_stars"].mean(),2)) +" ⭐")

    with subcol2:
        st.metric(label="Number of Ratings", value=str(reviewData.shape[0]) + " 🔎")

    st.markdown("#### Categories")

    annotated_text(processTagList(businessData["categories"][0].split(", ")))

    st.markdown("#### Attributes")

    annotated_text(processTagList(businessData["attributes_list"][0].split(",")))

    st.markdown("#### Location")

    st.markdown("##### " + businessData["address"][0] + ", " +
             businessData["city"][0] + ", " +
             businessData["state"][0])

    st.map(businessData)





