import io
import requests
import pandas as pd
import functions_framework
from flask import abort, Response, Request
from google.cloud import storage
from google.cloud.exceptions import NotFound

crime_endpoint = 'https://data.seattle.gov/resource/tazs-3rd5.json'
fire_endpoint = 'https://data.seattle.gov/resource/kzjm-xkqj.json'

@functions_framework.http
def extract_data(request: Request) -> Response:
    if request.method == 'GET':
        endpoint = request.args.get('endpoint')

        etl_run_type = request.headers.get('initial')

        if endpoint == 'crime':
            base = crime_endpoint
            orderby = 'offense_start_datetime'
        elif endpoint == 'fire':
            base = fire_endpoint
            orderby = 'datetime'
        else:
            abort(400, 'Invalid endpoint query parameter')

        if etl_run_type == 'True':
            print('Initial ETL')
        else:
            print('Incremental ETL')
        
        offset = 0

        all_data = pd.DataFrame()

        while True:
            if offset % 100000 == 0:
                print(f'offset: {offset}')
            response = requests.get(f'{base}?$offset={offset}&$order={orderby}%20ASC')
            if response.status_code == 200:
                data = response.json()
                if data:
                    thousand = pd.DataFrame(data)
                    all_data = pd.concat([all_data, thousand], axis = 0)
                    offset += 1000
                else:
                    break
            else:
                abort(response.status_code)
        
        write_to_gcs(all_data, f'{endpoint}_data.csv')
        return Response(status=204)

def write_to_gcs(df: pd.DataFrame, file_name: str) -> None:
    # Convert DataFrame to CSV in memory
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    # Set up GCS client and upload
    client = storage.Client()
    
    bucket_name = 'seattle-fire-and-crime'
    try:
        bucket = client.get_bucket(bucket_name)
    except NotFound:
        bucket = client.create_bucket(bucket_name)

    blob = bucket.blob(file_name)

    csv_buffer.seek(0)  # Rewind the buffer to the beginning
    blob.upload_from_file(csv_buffer, content_type='text/csv') 

    print(f'Wrote {file_name} blob to GCS bucket {bucket_name}')
