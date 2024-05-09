FROM bitnami/spark

WORKDIR /opt/bitnami/spark/yelp

COPY ./requirements.txt .

RUN pip install -r requirements.txt

CMD spark-submit --jars /jars/*.jar main.py
