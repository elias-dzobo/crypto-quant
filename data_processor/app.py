#imports
import os 
import psycopg2
from datetime import datetime, timedelta
from utils import * 
import boto3 
from dotenv import dotenv_values
import pandas as pd 
from airflow import DAG 
from airflow.operators.python import PythonOperator

#global vars
env = dotenv_values('../.env')

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'crypto_data_pipeline',
    default_args=default_args,
    description='A pipeline to fetch crypto data and store it in S3 and Postgres',
    schedule_interval=timedelta(days=7),
    catchup=False
)

# get historical data and convert to csv 
def get_historical_data(token):
    end = datetime.now().strftime('%Y-%m-%d')
    start = end - datetime.timedelta(days=7).strftime('%Y-%m-%d')
    url = f'https://api.exchange.coinbase.com/products/{token}/candles?start={start}&end={end}&granularity=86400' 
    data = get_json_response(url)
    return data


# save data to s3
def save_to_s3(token, data):
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    s3 = boto3.client('s3',
                  aws_access_key_id = env.get('AWS_ACCESS_KEY_ID'),
                  aws_secret_access_key=env.get('AWS_SECRET_ACCESS_KEY')) 
    
    file = pd.DataFrame(data, columns=['time', 'low', 'high', 'open', 'close', 'volume'])
    filename = f'temp/{token}_{today}.csv'
    file.to_csv(f'temp/{token}_{today}.csv')
    
    try:
        s3.upload_file(filename, env.get('AWS_BUCKET_NAME'), f'rawdata/{filename}')
        logging.info('Successfully uploaded')
    except Exception as e:
        logging.error(f'Error {e} occurred while uploading file')


# save data to postgres database
def save_to_postgres(token, data):
    conn = psycopg2.connect(
        host=env.get('DB_HOST'),
        database=env.get('DB_NAME'),
        user=env.get('DB_USER'),
        password=env.get('DB_PASSWORD')
    )
    data = [tuple(v) for v in data]
    check_and_create_table(conn, token)
    insert_values(conn, token, data)
    conn.close()

tokens = ['INJ', 'QNT', 'STORJ', 'VELO', 'SOL', 'JTO', 'ICP', 'SHIB', 'AUCTION', 'OCEAN', 'BONK', 'TIME', 'BTC', 'JUP', 'ILV']


# airflow pipeline 
for token in tokens:
    get_data_task = PythonOperator(
        task_id='get_historical_data',
        python_callable=get_historical_data,
        op_args=[token],
        dag=dag
    )

    save_s3_task = PythonOperator(
        task_id='save_to_s3',
        python_callable=save_to_s3,
        op_args=[token, "{{ ti.xcom_pull(task_ids='get_historical_data') }}"],
        dag=dag
    )

    save_postgres_task = PythonOperator(
        task_id='save_to_postgres',
        python_callable=save_to_postgres,
        op_args=[token, "{{ ti.xcom_pull(task_ids='get_historical_data') }}"],
        dag=dag
    )

# Define task dependencies
get_data_task >> [save_s3_task, save_postgres_task]