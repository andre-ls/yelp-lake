import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
import bq_queries as bigquery
import streamlit as st
from streamlit_searchbox import st_searchbox
from annotated_text import annotated_text
from wordcloud import WordCloud

st.set_page_config(layout='wide')


# Introduction
column_image, column_title = st.columns([0.3,2.0])
with column_image:
    st.image('yelp_logo.png',width=200)

with column_title:
    st.title('Yelp Business Data Insights')

# Search
df_search = bigquery.getBusinessNames()

def getBusinessNames(searchQuery):
    df_results = df_search[df_search['query_results'].str.match(searchQuery)]
    return list(zip(df_results['query_results'], df_results['business_id']))

randomBusinessId = bigquery.getRandomBusinessId()

businessId = st_searchbox(
    search_function=getBusinessNames,
    placeholder="Enter Business Name...",
    label="Search Business",
    clear_on_submit=False,
    default=randomBusinessId
)

# Data Loading and Processing

def processTagList(tagList):
    return [(x,"") for x in tagList]

def processCheckinTimeSeries(df):
    if(df.empty == False):
        df.index = pd.to_datetime(df["date"])
        idx = pd.date_range(min(df.date), max(df.date))
        df = df.drop("date",axis=1)
        df = df.reindex(idx, fill_value=0)
        return df.groupby(pd.Grouper(freq='ME')).sum("counts")
    else:
        return df

def processFrequentCustomersData(df):
    df.index = df.index + 1
    df["yelping_since"] = pd.to_datetime(df["yelping_since"]).dt.strftime('%m/%d/%Y')
    df.columns = ["Name","Cool","Funny","Reviews","Yelping Since","Checkins"]
    return df

businessData = bigquery.getBusinessData(businessId)
reviewData = bigquery.getReviewData(businessId)
reviewDistribution = bigquery.getReviewsDistribution(businessId)
checkinData = bigquery.getCheckinData(businessId)
frequentCustomersData = bigquery.getFrequentCustomersData(businessId)

checkinData = processCheckinTimeSeries(checkinData)
frequentCustomersData = processFrequentCustomersData(frequentCustomersData)
categoriesTagList = processTagList(businessData["categories"][0].split(", "))
attributesTagList = processTagList(businessData["attributes_list"][0].split(","))

# Data Visualization

col1, col2, col3 = st.columns(3)

with col1:

    st.subheader(businessData["name"][0])

    subcol1, subcol2 = st.columns(2)

    with subcol1:
        st.metric(label="Average Rating", value=str(np.round(reviewData["avg_stars"].mean(),2)) +" ⭐")

    with subcol2:
        st.metric(label="Number of Ratings", value=str(reviewData["number_reviews"][0]) + " 🔎")

    st.markdown("#### Categories")

    annotated_text(categoriesTagList)

    st.markdown("#### Attributes")

    annotated_text(attributesTagList)

    st.markdown("#### Location")

    st.markdown("##### " + businessData["address"][0] + ", " +
             businessData["city"][0] + ", " +
             businessData["state"][0])

    st.map(businessData)

with col2:

    st.markdown("#### Review Stars Distribution")

    fig = px.bar(
        reviewDistribution,
        x="stars",
        y="count",
        color_discrete_sequence =['#FF1A1A']*5,
        labels={
             "stars": "Stars",
             "count": "Count"
        },
        height=400
    )

    st.plotly_chart(fig, theme="streamlit", use_container_width=True)

    st.markdown("#### Checkins Time Series")

    fig = px.line(
        checkinData,
        x=checkinData.index,
        y="counts",
        color_discrete_sequence =['#FF1A1A'],
        labels={
             "month": "Months",
             "counts": "Checkins"
        },
        range_y=[0,checkinData["counts"].max()],
        height=450
    )

    st.plotly_chart(fig, theme="streamlit", use_container_width=True)

with col3:

    wordsDicts = reviewData["filtered_text"][0]["list"]
    wordsList = " ".join([d["element"] for d in wordsDicts])

#    wordsList = reviewData["filtered_text"][0].replace("[","").replace("]","")
#
    wordcloud = WordCloud(width=1600, height=1100,background_color=None,mode="RGBA").generate(wordsList)

    st.markdown("#### Most Commmon Words on Reviews")
    fig = plt.figure(figsize=(25,25),frameon=False)
    plt.axis("off")
    plt.imshow(wordcloud, interpolation='catrom')
    st.pyplot(fig)

    st.markdown("#### Top 10 Most Frequent Customers")
    st.dataframe(frequentCustomersData.head(10))

## Credits

with col1:

    st.markdown('## Find Me!')
    st.write("[![Buy me a coffee](https://img.shields.io/badge/GitHub-000013?style=for-the-badge&logo=github&logoColor=white&link=https://tr.linkedin.com/in/andr%C3%A9lamachado)](https://github.com/andre-ls)[![Connect](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white&link=https://tr.linkedin.com/in/andr%C3%A9lamachado)](https://tr.linkedin.com/in/andrélamachado)")
