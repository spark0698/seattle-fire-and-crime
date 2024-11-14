import requests
import pandas as pd
import functions_framework
from flask import abort, Response, Request
from google.cloud import storage
from google.cloud.exceptions import NotFound
from datetime import datetime

crime_endpoint = 'https://data.seattle.gov/resource/tazs-3rd5.json'
fire_endpoint = 'https://data.seattle.gov/resource/kzjm-xkqj.json'

@functions_framework.http
def extract_data(request: Request) -> Response:
    if request.method == 'GET':
        endpoint = request.args.get('endpoint')

        etl_run_type = request.headers.get('initial')

        # Determines which endpoint to extract from
        if endpoint == 'crime':
            base = crime_endpoint
            orderby = 'offense_start_datetime'
        elif endpoint == 'fire':
            base = fire_endpoint
            orderby = 'datetime'
        else:
            abort(400, 'Invalid endpoint query parameter')

        # TODO Determines type of ETL 
        if etl_run_type == 'True':
            print('Initial ETL')
        else:
            print('Incremental ETL')
        
        offset = 0

        # Write each page to temp CSV
        date = datetime.now().strftime("%Y%m%d-%H%M%S")
        temp_file = f'/tmp/{endpoint}_data_{date}.csv'
        while True:
            if offset == 0:
                head = True
            else:
                head = False
            page_data = get_api_data(offset, base, orderby)
            if page_data == []:
                break
            pd.DataFrame(page_data).to_csv(temp_file, mode='a', header = head, index=False)
            offset += 1000
        
        # Load full temp CSV to blob in Cloud Storage
        write_to_gcs(temp_file, f'{endpoint}_data.csv')
        return Response(status=204)
    
def get_api_data(offset: int, base: str, orderby:str) -> list:
    if offset % 100000 == 0:
        print(f'offset: {offset}')
    response = requests.get(f'{base}?$offset={offset}&$order={orderby}%20ASC')
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        abort(response.status_code)

def write_to_gcs(temp_csv: str, file_name: str) -> None:
    # Set up GCS client
    client = storage.Client()
    
    # Create bucket if doesn't exist
    bucket_name = 'seattle-fire-and-crime'
    try:
        bucket = client.get_bucket(bucket_name)
    except NotFound:
        bucket = client.create_bucket(bucket_name)

    # Create blob
    blob = bucket.blob(file_name)

    # Upload file into blob
    blob.upload_from_filename(temp_csv, content_type='text/csv') 

    print(f'Wrote {file_name} blob to GCS bucket {bucket_name}')
