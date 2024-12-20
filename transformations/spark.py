from sedona.spark import *
from sedona.sql import ST_GeomFromGeoJSON, ST_AsText
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType
from uuid import uuid4
import schemas as s
from filepaths import fire_file_path, crime_file_path, neighborhood_file_path 

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
    fire_data = load_data(fire_file_path, s.fire_schema) \
        .withColumn('incident_type', F.lit('fire'))

    crime_data = load_data(crime_file_path, s.crime_schema) \
        .withColumn('incident_type', F.lit('crime')) \
        .withColumnRenamed('_100_block_address', 'address') \
        .withColumnRenamed('offense_start_datetime', 'datetime')

    neighborhood_data = load_data(neighborhood_file_path, s.dim_neighborhood_schema)
    
    # Add neighborhood data
    print('Adding neighborhood data to fire and crime')
    fire_data_neighb = add_neighborhood(fire_data, neighborhood_data)
    crime_data_neighb = add_neighborhood(crime_data, neighborhood_data)

    fire_data_prep = add_missing_columns(fire_data_neighb, s.all_incidents_schema) \
        .select(*s.all_incidents_schema.fieldNames())
    crime_data_prep = add_missing_columns(crime_data_neighb, s.all_incidents_schema) \
        .select(*s.all_incidents_schema.fieldNames())

    # Combine all incident data
    print('Combining fire and crime data')
    all_incidents = fire_data_prep.union(crime_data_prep)
    total_incident_count = all_incidents.count()
    print(f'total incidents pre star {total_incident_count}')

    # Create dim_neighborhood
    print('Creating dim_neighborhood table')
    dim_neighborhood = neighborhood_data \
        .drop_duplicates(['geometry', 'district', 'neighborhood']) \
        .withColumn('neighborhood_id', F.hash(F.concat(F.col('district'), F.col('neighborhood')))) \
        .select(*s.dim_neighborhood_schema.fieldNames())
    
    dim_neighborhood_wkt = dim_neighborhood.withColumn('geometry', ST_AsText(F.col('geometry')))

    # Create dim_date
    print('Creating dim_date')
    dim_date = all_incidents \
        .select('datetime', 'offense_end_datetime', 'report_datetime') \
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
    print('Creating dim_incident_type')
    dim_incident_type = all_incidents \
        .select('incident_type') \
        .distinct() \
        .withColumn('incident_type_id', F.monotonically_increasing_id()) \
        .select(*s.dim_incident_type_schema.fieldNames()) 
    row_count_incident = dim_incident_type.count()
    print(f'dim_incident_type rows {row_count_incident}')    

    # Create fact table
    print('Creating fact_incident table')
    fact_incident1 = all_incidents \
        .join(dim_neighborhood, ['geometry', 'district', 'neighborhood'], 'left')
    
    fact_incident2 = fact_incident1 \
        .join(dim_date, ['datetime', 'offense_end_datetime', 'report_datetime'], 'left')
    
    fact_incident3 = fact_incident2 \
        .join(dim_incident_type, 'incident_type', 'left') \
        .select(*s.fact_incident_schema.fieldNames())
    
    fact1_count = fact_incident1.count()
    fact2_count = fact_incident2.count()
    fact3_count = fact_incident3.count()
    print(f'fact rows after neighborhood join {fact1_count}')
    print(f'fact rows after datetime join {fact2_count}')
    print(f'fact rows after incident type join {fact3_count}')

    dfs = {'dim_incident_type': dim_incident_type,
           'dim_neighborhood': dim_neighborhood_wkt,
           'dim_date': dim_date,
           'fact_incident': fact_incident3}

    # Write fact table
    print('Writing to bigquery')
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
                .withColumn('district', F.expr("properties['L_HOOD']")) \
                .withColumn('neighborhood', F.expr("properties['S_HOOD']")) \
                .withColumn('geometry', F.col('geometry')) \
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
            .option('writeMethod', 'direct') \
            .mode(m) \
            .save(f'seattle_dataset.{name}')

def add_neighborhood(df: DataFrame, neighb_info: DataFrame) -> DataFrame:
    point_df = df.withColumn('point', ST_Point(df.longitude, df.latitude))
    neighb_df = point_df.alias('point_df') \
        .join(neighb_info.alias('neighb_info'), ST_Within(point_df.point, neighb_info.geometry), 'left') 

    return neighb_df

def add_missing_columns(df: DataFrame, schema: StructType) -> DataFrame:
    df_columns = set(df.columns)
    target_columns = set([field.name for field in schema.fields])

    missing_columns = target_columns - df_columns

    for col in missing_columns:
        # Find the type of the missing column from the target schema
        column_type = dict((field.name, field.dataType) for field in schema.fields).get(col)
        if column_type:
            df = df.withColumn(col, F.lit(None).cast(column_type))
    
    return df

if __name__ == '__main__':
    main()
