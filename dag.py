from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
from bs4 import BeautifulSoup
import json
import csv
import logging
import boto3
from etl import ETL,WD,WDV,WV,PV,TAD,Clouds,AS,get_weather_info,parse_metar_data,fetch_metar_data,get_weather

def upload_to_s3(csv_file_path, bucket_name, key):
    s3 = boto3.client('s3')
    s3.upload_file(csv_file_path, bucket_name, key)

# Define the default_args dictionary to specify the start date, owner, and other parameters.
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate a DAG with the provided default_args.
dag = DAG(
    'metar_etl_and_upload_to_s3',
    default_args=default_args,
    description='A DAG to extract METAR data, process it, and upload to S3',
    schedule_interval=timedelta(days=1),  # You can adjust the schedule_interval as needed.
)

# Create task instances for the functions.
etl_task = PythonOperator(
    task_id='run_etl',
    python_callable=get_weather,
    dag=dag,
)

upload_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    provide_context=True,
    op_args=['output.csv','akash-airflow-metar-bucket', 'output.csv'],  # Replace with your S3 bucket and key
    dag=dag,
)

# Set the task dependencies.
etl_task >> upload_task