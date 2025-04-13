from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator


def ingest_bronze_func():
    from library.ingest import Ingest
    ingest = Ingest()
    ingest.ingest_bronze('news_api', 'top_headlines')

with DAG('top_headlines', start_date=datetime(2025,4,13), schedule_interval='@daily', catchup=False):
    
    ingest_bronze = PythonOperator(
        task_id='ingest_bronze',
        python_callable=ingest_bronze_func
        )