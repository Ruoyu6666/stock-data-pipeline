from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from spark.connector import SparkSession


import pandas as pd
import yfinance as yf



default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
    

with DAG(
    dag_id='data_ingestion',
    default_args=default_args,
    description='A DAG for data ingestion',
    start_date=datetime(2025, 6, 18),
    schedule='@daily',
    catchup=False,
) as dag:



    ingest_task = PythonOperator(
        task_id='data_extraction',
        python_callable=data_extraction,
    )