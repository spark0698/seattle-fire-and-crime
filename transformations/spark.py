from sedona.spark import *
from sedona.sql import ST_GeomFromGeoJSON, ST_AsText
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, year, month, dayofmonth, hour, minute, unix_timestamp, expr, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampNTZType, BinaryType
from uuid import uuid4
from schemas import fire_schema, crime_schema, dim_incident_type_schema
from filepaths import fire_file_path, crime_file_path, neighborhood_file_path 

import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/Users/heepark/Sean/seattle-fire-and-crime/gcp/seattle-fire-and-crime-9ee8045e549b.json'

spark = SparkSession.builder.appName('SeattleIncidents') \
    .getOrCreate()

sedona = SedonaContext.create(spark)

# Temporary GCS bucket for BigQuery export data
bucket = 'seattle-fire-and-crime'
spark.conf.set('temporaryGcsBucket', bucket)

def main():
    fire_data = spark.read.csv(fire_file_path, header = True, schema = fire_schema) \
            .withColumn('incident_type', lit('fire'))
    
    crime_data = spark.read.csv(crime_file_path, header = True, schema = crime_schema) \
            .withColumn('incident_type', lit('crime'))

    neighborhood_data = sedona.read.format('geojson').option('multiLine', 'true').load(neighborhood_file_path) \
            .selectExpr('explode(features) as features') \
            .select('features.*') \
            .withColumn('district', expr("properties['L_HOOD']")) \
            .withColumn('neighborhood', expr("properties['S_HOOD']")) \
            .withColumn('geometry', col('geometry')) \
            .drop('properties') \
            .drop('type')
    
    fire_data_neighb = add_neighborhood(fire_data, neighborhood_data)
    crime_data_neighb = add_neighborhood(crime_data, neighborhood_data)
    
    fire_data_neighb.show(2)
    crime_data_neighb.show(2)

    dim_neighborhood = read_from_bigquery('dim_neighborhood')
    dim_neighborhood.show(2)
    
    # dfs = {'fire_data': fire_data, 
    #         'crime_data': crime_data, 
    #         'neighborhood_data': neighborhood_data}

    # write_to_bigquery(dfs)

    spark.stop()

def load_data(filename: str, schema_name: StructType) -> DataFrame:
    filetype = filename.split('.')[-1]
    if filetype == 'csv':
        return spark.read.csv(filename, header = True, schema = schema_name)
    elif filetype == 'geojson':
        df = sedona.read.format('geojson').option('multiLine', 'true').load(filename) \
                .selectExpr('explode(features) as features') \
                .select('features.*') \
                .withColumn('district', expr("properties['L_HOOD']")) \
                .withColumn('neighborhood', expr("properties['S_HOOD']")) \
                .withColumn('geometry', col('geometry')) \
                .drop('properties') \
                .drop('type')
        
        return df
    else:
        raise Exception('Unsupported filetype load attempted')

def read_from_bigquery(table_name: str) -> DataFrame:
    try:
        df = spark.read.format('bigquery') \
            .option('table', f'seattle_dataset.{table_name}')
    except Exception as e:
        if 'NotFound' in str(e):
            df = spark.createDataFrame([])
        raise e
    return df

def write_to_bigquery(dfs: dict):
    # Save the data to BigQuery (overwriting for now before incremental batch load is implemented)
    for name, df in dfs.items():
        df.write.format('bigquery') \
            .option('table', f'seattle_dataset.{name}') \
            .mode('overwrite') \
            .save()

def add_neighborhood(df: DataFrame, neighb_info: DataFrame) -> DataFrame:
    point_df = df.withColumn('point', ST_Point(df.longitude, df.latitude))
    neighb_df = point_df.alias('point_df') \
        .join(neighb_info.alias('neighb_info'), ST_Within(point_df.point, neighb_info.geometry)) 

    return neighb_df

if __name__ == '__main__':
    main()