FROM apache/airflow:2.8.0

ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update -qq && apt-get install vim -qqq
# git gcc g++ -qqq

COPY requirements.txt .
USER $AIRFLOW_UID
RUN pip install --no-cache-dir -r requirements.txt

