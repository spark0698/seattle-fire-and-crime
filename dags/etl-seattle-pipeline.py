import os
from dotenv import load_dotenv
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from datetime import timedelta, datetime

load_dotenv()

# Define DAG
schedule_interval = '@monthly'
start_date = days_ago(1)

# Dataproc spark batch job config
PYSPARK_SCRIPT_URI = 'gs://seattle-fire-and-crime/spark.py'
REGION = 'us-west2'
BUCKET = 'seattle-fire-and-crime'
PROJECT_ID = 'seattle-fire-and-crime'
FILES_URIS = [
    'gs://seattle-fire-and-crime/schemas.py',
    'gs://seattle-fire-and-crime/filepaths.py'
]
JARS_URIS = [
    'gs://seattle-fire-and-crime/jars/geotools-wrapper-1.7.0-28.5.jar',
    'gs://seattle-fire-and-crime/jars/sedona-spark-shaded-3.5_2.13-1.7.0.jar'
]
CONTAINER_IMAGE = 'us-west2-docker.pkg.dev/seattle-fire-and-crime/seattle-repo/sedona'
VERSION = '2.2'

# Define DAG
default_args = {
    'owner' : 'admin',
    'retries' : 0,
    'retry_delay' : timedelta(minutes = 5)
}

dag = DAG(
    'seattle-fire-and-crime-pipeline',
    description = 'ETL pipeline for Seattle Fire and Crime Incidents',
    default_args = default_args,
    schedule_interval = schedule_interval,
    start_date = start_date,
    catchup = True,
    max_active_runs = 1
)

# GCP config variables
GCP_CONN_ID = 'google_cloud_default'

# Cloud Run API load tasks
t1 = SimpleHttpOperator(
    task_id = 'trigger_cloud_run_crime',
    http_conn_id = 'cloud_run_http',
    method = 'GET', 
    data = {'endpoint': 'crime'},
    response_check = lambda response: response.status_code == 204,  # Check for successful response
    dag = dag
)

t2 = SimpleHttpOperator(
    task_id = 'trigger_cloud_run_fire',
    http_conn_id = 'cloud_run_http',
    method = 'GET', 
    data = {'endpoint': 'fire'},
    response_check = lambda response: response.status_code == 204,  # Check for successful response
    dag = dag
)

# Submit batch job to dataproc task
t3 = DataprocCreateBatchOperator(
    task_id = 'submit_pyspark_job',
    region = REGION,
    batch = {
        'pyspark_batch': {
            'main_python_file_uri': PYSPARK_SCRIPT_URI,
            'python_file_uris': FILES_URIS,
            'jar_file_uris': JARS_URIS
        },
        'runtime_config': {
            'version': '2.2',
            'container_image': CONTAINER_IMAGE
        }
    },
    gcp_conn_id = GCP_CONN_ID,
    dag = dag
)

t1 >> t3
t2 >> t3
