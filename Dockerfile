FROM apache/airflow:2.8.1

USER root
RUN apt-get update && \
    apt-get install -y default-jre-headless procps && \
    apt-get clean

USER airflow
# Ajout de psycopg2-binary pour la connexion Postgres
RUN pip install pyspark==3.5.0 apache-airflow-providers-apache-spark psycopg2-binary