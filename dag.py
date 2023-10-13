from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.decorators import dag, task
from etl import read_csv, transform_csv, read_db, transform_db, merge, load, store


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 14),  # Update the start date to today or an appropriate date
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1) #It do the retries one minute before the first time, and it do it just one more time because the key "retries" said it to do it just once (the line before this one)
}


@dag(
    default_args=default_args,
    description='DAG for ETL workshop002',
    schedule_interval='@daily',  # Set the schedule interval as per your requirements
)

def etl_workshop():

    @task
    def extract_task_csv ():
        return read_csv()

    @task
    def transform_task_csv (json_data):
        return transform_csv(json_data)
    
    @task
    def extract_task_db ():
        return read_db()
    
    @task
    def transform_task_db (json_data):
        return transform_db(json_data)
    
    @task
    def merge_task(json_data1, json_data2):
        return merge(json_data1, json_data2)

    @task
    def load_task(json_data):
        load(json_data)

    @task
    def store_task(json_data):
        store(json_data)

    data = extract_task_csv()
    data_tcsv = transform_task_csv(data)
    data_db=extract_task_db ()
    data_tdb = transform_task_db(data_db)
    merge_data = merge_task(data_tcsv, data_tdb)
    load_data = load_task(merge_data)
    store_task(load_data)



etl_workshop()