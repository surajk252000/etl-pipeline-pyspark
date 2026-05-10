FROM apache/airflow:2.7.0-python3.9

USER root
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
RUN pip install pyspark==3.5.0 mysql-connector-python boto3