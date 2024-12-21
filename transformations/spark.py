from sedona.spark import *
from sedona.sql import ST_GeomFromGeoJSON, ST_AsText
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, DecimalType, TimestampNTZType, BinaryType
from typing import Optional
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
        .withColumn('incident_type', F.lit('fire')) \
        .drop_duplicates(['incident_number'])

    # Crime data when read from data sometimes has offense_end_datetime value in the wrong column
    crime_data_initial = load_data(crime_file_path, infer_schema=False) \
        .withColumn('incident_type', F.lit('crime')) \
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
        .withColumn('offense_start_datetime', F.col('offense_start_datetime').cast(TimestampNTZType())) \
        .withColumn('report_datetime', F.col('report_datetime').cast(TimestampNTZType())) \
        .withColumn('longitude', F.col('longitude').cast(DecimalType(25, 20))) \
        .withColumn('latitude', F.col('latitude').cast(DecimalType(25, 20))) \
        .withColumn('offense_end_datetime', F.col('offense_end_datetime').cast(TimestampNTZType()))

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
    all_incidents = fire_data_prep.union(crime_data_prep) \
        .withColumn('incident_id', F.monotonically_increasing_id())

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
    print('Creating dim_incident_type')
    dim_incident_type = all_incidents \
        .select('incident_type') \
        .distinct() \
        .withColumn('incident_type_id', F.monotonically_increasing_id()) \
        .select(*s.dim_incident_type_schema.fieldNames()) 

    # Create fact table
    print('Creating fact_incident table')

    fact_incident = all_incidents \
        .join(dim_date, all_incidents['datetime'].eqNullSafe(dim_date['datetime']), 'inner') \
        .join(dim_neighborhood, ['geometry', 'district', 'neighborhood'], 'left') \
        .join(dim_incident_type, 'incident_type', 'left') \
        .select(*s.fact_incident_schema.fieldNames())

    dfs = {'dim_incident_type': dim_incident_type,
           'dim_neighborhood': dim_neighborhood_wkt,
           'dim_date': dim_date,
           'fact_incident': fact_incident}

    # Write fact table
    print('Writing to bigquery')
    write_to_bigquery(dfs, 'overwrite')

    spark.stop()

def load_data(filename: str, schema_name: Optional[StructType], infer_schema: bool = True) -> DataFrame:
    filetype = filename.split('.')[-1]
    if filetype == 'csv':
        if infer_schema:
            return spark.read.csv(filename, header = True, schema = schema_name)
        else:
            return spark.read.csv(filename, header = True)
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
    
def reorder_row_if_needed(*args) -> list[str]:
    values = list(args)
    is_latitude = False

    if values[-1] is not None:
        try:
            # Attempt to convert the last value to a float (latitude)
            latitude = float(values[-1])  # Latitude is expected to be a decimal number
            is_latitude = True
        except ValueError:
            is_latitude = False

    if is_latitude:
        temp = values[3]
        for i in range(4, 17): 
            values[i - 1] = values[i]

        values[16] = temp
    
    return values

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
        .join(F.broadcast(neighb_info).alias('neighb_info'), ST_Within(point_df.point, neighb_info.geometry), 'left') 

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
