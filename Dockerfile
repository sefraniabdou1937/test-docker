# On part de ton image Airflow
FROM apache/airflow:2.8.1

# On installe Java (obligatoire pour faire tourner Spark)
USER root
RUN apt-get update && \
    apt-get install -y default-jre-headless && \
    apt-get clean

# On installe PySpark ET le provider Spark pour Airflow
USER airflow
RUN pip install pyspark==3.5.0 apache-airflow-providers-apache-spark