from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from datetime import timedelta, datetime

schedule_interval = '@daily'
start_date = days_ago(1)

default_args = {
    'owner' : 'airflow',
    'retries' : 1,
    'retry_delay' : timedelta(minutes = 5)
}

dag = DAG(
    'seattle-fire-and-crime-pipeline',
    default_args = default_args,
    schedule_interval = schedule_interval,
    start_date = start_date,
    catchup = True,
    max_active_runs = 1
)