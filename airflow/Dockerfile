FROM apache/airflow:3.2.2-python3.12

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

COPY requirements-airflow.txt /tmp/requirements-airflow.txt

RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt
