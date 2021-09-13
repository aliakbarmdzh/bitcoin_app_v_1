from datetime import datetime, timedelta
from airflow import DAG
import json

from etl_code import extract_exchange_data, load_exchange_data, transform_exchange_data
from airflow.operators.python import PythonOperator

default_arguments = {
    "owner": "aliakbar",
    "depends_on_past": False,
    "wait_for_downstram": False,
    "start_date": datetime(2021, 9, 11),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)

}

dag = DAG(
    dag_id="automation",
    default_args=default_arguments,
    schedule_interval="*/1 * * * *"
)

task1 = PythonOperator(
    task_id='extract_exchange_data',
    python_callable=extract_exchange_data,
    dag=dag
)

task2 = PythonOperator(
    task_id='transform_exchange_data',
    python_callable=transform_exchange_data,
    dag=dag
)

task3 = PythonOperator(
    task_id='load_exchange_data',
    python_callable=load_exchange_data,
    dag=dag
)

task1 >> task2 >> task3
