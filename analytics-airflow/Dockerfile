FROM apache/airflow:2.8.1

WORKDIR /opt/airflow

COPY ../requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt