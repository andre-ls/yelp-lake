FROM bitnami/spark

WORKDIR /opt/bitnami/spark/yelp

COPY ./requirements.txt .

ENV GOOGLE_APPLICATION_CREDENTIALS="/opt/bitnami/spark/yelp/secrets/gcp_key.json"

RUN pip install -r requirements.txt

CMD spark-submit --driver-memory 8g --jars /opt/bitnami/spark/yelp/jars/gcs-connector-hadoop3-latest.jar --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem main.py
