from sedona.spark import *
from sedona.sql import ST_AsText
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, DecimalType, TimestampNTZType
from typing import Optional
import schemas as s
from filepaths import fire_file_path, crime_file_path, neighborhood_file_path 
from data_cleaning import add_missing_columns, add_neighborhood, reorder_row_if_needed
from data_flow import load_data, write_to_bigquery

spark = SparkSession.builder.appName('SeattleIncidents') \
    .getOrCreate()

sedona = SedonaContext.create(spark)

project_id = 'seattle-fire-and-crime'
dataset_id = 'seattle_dataset'

# Temporary GCS bucket for BigQuery export data
bucket = 'seattle-fire-and-crime'
spark.conf.set('temporaryGcsBucket', bucket)
spark.conf.set("spark.sql.adaptive.enabled", "true")

def main():
    fire_data = load_data(spark, sedona, fire_file_path, s.fire_schema) \
        .withColumn('incident_type', F.lit('fire')) \
        .withColumnRenamed('type', 'fire_type') \
        .drop_duplicates(['incident_number'])

    # Crime data when read from data sometimes has offense_end_datetime value in the wrong column
    crime_data_initial = load_data(spark, sedona, crime_file_path, infer_schema=False) \
        .withColumnRenamed('_100_block_address', 'address') \
        .withColumnRenamed('offense_start_datetime', 'datetime') \
        .drop_duplicates(['report_number'])
    
    columns = crime_data_initial.columns
    
    reorder_row_udf = F.udf(reorder_row_if_needed, F.ArrayType(F.StringType()))
    df_corrected = crime_data_initial.withColumn('corrected_row', reorder_row_udf(*columns))
    df_final = df_corrected.selectExpr(
        ['corrected_row[{}] as {}'.format(i, columns[i]) for i in range(len(columns))]
    )

    crime_data = df_final \
        .withColumn('datetime', F.col('datetime').cast(TimestampNTZType())) \
        .withColumn('report_datetime', F.col('report_datetime').cast(TimestampNTZType())) \
        .withColumn('longitude', F.col('longitude').cast(DecimalType(25, 20))) \
        .withColumn('latitude', F.col('latitude').cast(DecimalType(25, 20))) \
        .withColumn('offense_end_datetime', F.col('offense_end_datetime').cast(TimestampNTZType())) \
        .withColumn('incident_type', F.lit('crime'))

    neighborhood_data = load_data(spark, sedona, neighborhood_file_path, s.dim_neighborhood_schema)
    
    # Add neighborhood data
    fire_data_neighb = add_neighborhood(fire_data, neighborhood_data)
    crime_data_neighb = add_neighborhood(crime_data, neighborhood_data)

    fire_data_prep = add_missing_columns(fire_data_neighb, s.all_incidents_schema) \
        .select(*s.all_incidents_schema.fieldNames())
    crime_data_prep = add_missing_columns(crime_data_neighb, s.all_incidents_schema) \
        .select(*s.all_incidents_schema.fieldNames())

    # Combine all incident data
    all_incidents = fire_data_prep.union(crime_data_prep) \
        .withColumn('incident_id', F.monotonically_increasing_id())

    # Create dim_neighborhood
    dim_neighborhood = neighborhood_data \
        .drop_duplicates(['geometry', 'district', 'neighborhood']) \
        .withColumn('neighborhood_id', F.hash(F.concat(F.col('district'), F.col('neighborhood')))) \
        .select(*s.dim_neighborhood_schema.fieldNames())
    
    dim_neighborhood_wkt = dim_neighborhood.withColumn('geometry', ST_AsText(F.col('geometry')))

    # Create dim_date
    dim_date = all_incidents \
        .select('datetime') \
        .drop_duplicates(['datetime']) \
        .withColumn('date_id', F.monotonically_increasing_id()) \
        .withColumn('year', F.year('datetime')) \
        .withColumn('month', F.month('datetime')) \
        .withColumn('day', F.dayofmonth('datetime')) \
        .withColumn('hour', F.hour('datetime')) \
        .withColumn('minute', F.minute('datetime')) \
        .withColumn('second', F.second('datetime')) \
        .withColumn('day_of_week', F.dayofweek('datetime')) \
        .withColumn('week_of_year', F.weekofyear('datetime')) \
        .withColumn('weekday_name', F.date_format('datetime', 'EEEE')) \
        .withColumn('month_name', F.date_format('datetime', 'MMMM')) \
        .withColumn('quarter', F.floor((F.month('datetime') - 1) / 3) + 1) \
        .select(*s.dim_date_schema.fieldNames())

    # Create dim_incident_type
    dim_incident_type = all_incidents \
        .select('incident_type') \
        .distinct() \
        .withColumn('incident_type_id', F.monotonically_increasing_id()) \
        .select(*s.dim_incident_type_schema.fieldNames()) 
    
    # Create dim_crime_details
    dim_crime_details = all_incidents \
        .select(
            'report_number', 'offense_id', 'report_datetime', 'group_a_b', 'crime_against_category', 
            'offense_parent_group', 'offense', 'offense_code', 'precinct', 'sector', 'beat', 'mcpp', 
            'offense_end_datetime'
            ) \
        .withColumn('crime_detail_id', F.monotonically_increasing_id()) \
        .select(*s.dim_crime_details_schema.fieldNames())
    
    # Create dim_fire_details
    dim_fire_details = all_incidents \
        .select('fire_type', 'report_location', 'incident_number') \
        .withColumn('fire_detail_id', F.monotonically_increasing_id()) \
        .select(*s.dim_fire_details_schema.fieldNames())

    # Create fact table
    fact_incident = all_incidents \
        .join(dim_date, all_incidents['datetime'].eqNullSafe(dim_date['datetime']), 'inner') \
        .join(dim_neighborhood, ['geometry', 'district', 'neighborhood'], 'left') \
        .join(dim_incident_type, 'incident_type', 'left') \
        .join(dim_crime_details, 'report_number', 'left') \
        .join(dim_fire_details, 'incident_number', 'left') \
        .select(*s.fact_incident_schema.fieldNames())

    dfs = {'dim_incident_type': dim_incident_type,
           'dim_neighborhood': dim_neighborhood_wkt,
           'dim_date': dim_date,
           'dim_crime_details': dim_crime_details,
           'dim_fire_details': dim_fire_details,
           'fact_incident': fact_incident}

    # Write tables to bigquery
    write_to_bigquery(dfs, 'overwrite')

    spark.stop()

if __name__ == '__main__':
    main()
