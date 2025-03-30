from airflow import DAG
from datetime import datetime
from library.ingest import Ingest
from airflow.operators.python import PythonOperator

ingest = Ingest()

with DAG('top_headlines', start_date=datetime(2025,3,30), schedule_interval='@daily', catchup=False):
    
    ingest_bronze = PythonOperator(
        task_id='ingest_bronze',
        python_callable=Ingest.ingest_bronze,
        op_kwargs={
            'source_name' : 'news_api',
            'dataset' : 'top_headlines'
        })