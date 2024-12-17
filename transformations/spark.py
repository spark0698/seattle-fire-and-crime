from sedona.spark import *
from sedona.sql import ST_GeomFromGeoJSON, ST_AsText
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, year, month, dayofmonth, hour, minute, unix_timestamp, expr, lit, concat, hash
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampNTZType, BinaryType
from uuid import uuid4
import schemas as s
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
    fire_data = spark.read.csv(fire_file_path, header = True, schema = s.fire_schema) \
            .withColumn('incident_type', lit('fire'))
    
    crime_data = spark.read.csv(crime_file_path, header = True, schema = s.crime_schema) \
            .withColumn('incident_type', lit('crime')) \
            .withColumnRenamed('_100_block_address', 'address') \
            .withColumnRenamed('offense_start_datetime', 'datetime')

    neighborhood_data = sedona.read.format('geojson').option('multiLine', 'true').load(neighborhood_file_path) \
            .selectExpr('explode(features) as features') \
            .select('features.*') \
            .withColumn('district', expr("properties['L_HOOD']")) \
            .withColumn('neighborhood', expr("properties['S_HOOD']")) \
            .withColumn('geometry', col('geometry')) \
            .drop('properties') \
            .drop('type')
    
    # Add neighborhood data
    fire_data_neighb = add_neighborhood(fire_data, neighborhood_data)
    crime_data_neighb = add_neighborhood(crime_data, neighborhood_data)

    fire_data_prep = add_missing_columns(fire_data_neighb, s.all_incidents_schema) \
        .select(*s.all_incidents_schema.fieldNames())
    crime_data_prep = add_missing_columns(crime_data_neighb, s.all_incidents_schema) \
        .select(*s.all_incidents_schema.fieldNames())

    # Combine all incident data
    all_incidents = fire_data_prep.union(crime_data_prep)

    # Update dim_neighborhood with any unique neighborhoods in incidents
    dim_neighborhood_read = read_from_bigquery('dim_neighborhood')
    dim_neighborhood = all_incidents \
        .drop_duplicates(['geometry', 'district', 'neighborhood']) \
        .withColumn('geometry', ST_AsText(col('geometry'))) \
        .withColumn('neighborhood_id', hash(concat(col('geometry'), col('district'), col('neighborhood')))) \
        .select(*s.dim_neighborhood_schema.fieldNames())
    
    dim_neighborhood_union = dim_neighborhood_read.union(dim_neighborhood)

    # Create fact table
    fact_incident = all_incidents.join(dim_neighborhood_union, ['geometry', 'district', 'neighborhood'], 'left') \
        .drop('geometry', 'district', 'neighborhood')

    dfs = {'dim_neighborhood': dim_neighborhood_union, 
        'fact_incident': fact_incident}

    write_to_bigquery(dfs, 'overwrite')

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
            .option('table', f'seattle_dataset.{table_name}') \
            .load()
    except Exception as e:
        print(type(e))
        print(e)
        df = spark.createDataFrame([], s.dim_neighborhood_schema)
    return df

def write_to_bigquery(dfs: dict, m: str):
    # Save the data to BigQuery (overwriting for now before incremental batch load is implemented)
    for name, df in dfs.items():
        df.write.format('bigquery') \
            .option('table', f'seattle_dataset.{name}') \
            .mode(m) \
            .save()

def add_neighborhood(df: DataFrame, neighb_info: DataFrame) -> DataFrame:
    point_df = df.withColumn('point', ST_Point(df.longitude, df.latitude))
    neighb_df = point_df.alias('point_df') \
        .join(neighb_info.alias('neighb_info'), ST_Within(point_df.point, neighb_info.geometry)) 

    return neighb_df

def add_missing_columns(df: DataFrame, schema: StructType) -> DataFrame:
    df_columns = set(df.columns)
    target_columns = set([field.name for field in schema.fields])

    missing_columns = target_columns - df_columns

    for col in missing_columns:
        # Find the type of the missing column from the target schema
        column_type = dict((field.name, field.dataType) for field in schema.fields).get(col)
        if column_type:
            df = df.withColumn(col, lit(None).cast(column_type))
    
    return df

if __name__ == '__main__':
    main()