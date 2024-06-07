FROM apache/airflow:2.5.1-python3.8

USER root
RUN apt-get update && apt-get install -y \
    libpq-dev

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dags /usr/local/airflow/dags

ENTRYPOINT ["airflow", "standalone"]
