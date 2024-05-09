FROM bitnami/spark

WORKDIR /opt/bitnami/spark/yelp

RUN pip install python-dotenv

CMD spark-submit --jars /jars/*.jar main.py
