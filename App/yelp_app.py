import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
import athena_queries as athena
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
def getBusinessNames(searchQuery):
    df = athena.getBusinessNames(searchQuery)
    return list(zip(df['query_results'], df['business_id']))

randomBusinessId = athena.getRandomBusinessId()

businessId = st_searchbox(
    search_function=getBusinessNames,
    placeholder="Enter Business Name...",
    label="Search Business",
    clear_on_submit=False,
    clearable=True,
    default=randomBusinessId
)

# Data Visualization

businessData = athena.getBusinessData(businessId)
reviewData = athena.getReviewData(businessId)
reviewDistribution = athena.getReviewsDistribution(businessId)
checkinData = athena.getCheckinData(businessId)
frequentCustomersData = athena.getFrequentCustomersData(businessId)
frequentCustomersData.index = frequentCustomersData.index + 1

def processTagList(tagList):
    return [(x,"") for x in tagList]

col1, col2, col3 = st.columns(3)

with col1:

    st.subheader(businessData["name"][0])

    subcol1, subcol2 = st.columns(2)

    with subcol1:
        st.metric(label="Average Rating", value=str(np.round(reviewData["avg_stars"].mean(),2)) +" ⭐")

    with subcol2:
        st.metric(label="Number of Ratings", value=str(reviewData["number_reviews"][0]) + " 🔎")

    st.markdown("#### Categories")

    annotated_text(processTagList(businessData["categories"][0].split(", ")))

    st.markdown("#### Attributes")

    annotated_text(processTagList(businessData["attributes_list"][0].split(",")))

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

    fig = px.bar(
        checkinData,
        x="month",
        y="counts",
        color_discrete_sequence =['#FF1A1A'],
        labels={
             "month": "Months",
             "counts": "Checkins"
        },
        height=450
    )

    st.plotly_chart(fig, theme="streamlit", use_container_width=True)

with col3:

    wordsList = reviewData["filtered_text"][0].replace("[","").replace("]","")

    wordcloud = WordCloud(height=280,background_color=None,mode="RGBA").generate(wordsList)

    st.markdown("#### Most Commmon Words on Reviews")
    fig = plt.figure(dpi=2400,frameon=False)
    plt.axis("off")
    plt.imshow(wordcloud, interpolation='spline36')
    st.pyplot(fig)


    st.markdown("#### Top 10 Most Frequent Customers")
    st.dataframe(frequentCustomersData.head(10))

## Credits

with col1:

    st.markdown('## Find Me!')
    st.write("[![Buy me a coffee](https://img.shields.io/badge/GitHub-000013?style=for-the-badge&logo=github&logoColor=white&link=https://tr.linkedin.com/in/andr%C3%A9lamachado)](https://github.com/andre-ls)[![Connect](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white&link=https://tr.linkedin.com/in/andr%C3%A9lamachado)](https://tr.linkedin.com/in/andrélamachado)")
