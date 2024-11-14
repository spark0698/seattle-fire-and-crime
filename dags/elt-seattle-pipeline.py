import os
from dotenv import load_dotenv
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from datetime import timedelta, datetime

load_dotenv()

# Define DAG
schedule_interval = '@daily'
start_date = days_ago(1)

default_args = {
    'owner' : 'airflow',
    'retries' : 0,
    'retry_delay' : timedelta(minutes = 5),
    'start_date': datetime(2024, 11, 1)
}

dag = DAG(
    'seattle-fire-and-crime-pipeline',
    description = 'Trigger Cloud Run, fetch data, move to BigQuery, and transform',
    default_args = default_args,
    schedule_interval = schedule_interval,
    start_date = start_date,
    catchup = True,
    max_active_runs = 1
)

# Config variables
GCP_CONN_ID = 'gcp_conn'

# Cloud Run API load tasks
t1 = SimpleHttpOperator(
    task_id = 'trigger_cloud_run_crime',
    http_conn_id = 'CLOUD_RUN_HTTP',
    method = 'GET', 
    data = {'endpoint': 'crime'},
    response_check = lambda response: response.status_code == 200,  # Check for successful response
    dag = dag
)

t2 = SimpleHttpOperator(
    task_id = 'trigger_cloud_run_fire',
    http_conn_id = 'CLOUD_RUN_HTTP',
    method = 'GET', 
    data = {'endpoint': 'fire'},
    response_check = lambda response: response.status_code == 200,  # Check for successful response
    dag = dag
)

t2.set_upstream(t1)