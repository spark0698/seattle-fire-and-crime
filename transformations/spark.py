from sedona.spark import *
from sedona.sql import ST_GeomFromGeoJSON, ST_AsText
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, hour, minute, unix_timestamp, expr, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampNTZType

spark = SparkSession.builder.appName('SeattleIncidents') \
    .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar') \
    .config('spark.jars.packages',
           'org.apache.sedona:sedona-spark-shaded-3.5_2.12-1.6.1,'
           'org.datasyslab:geotools-wrapper:1.6.1-28.2') \
    .getOrCreate()

sedona = SedonaContext.create(spark)

# Temporary GCS bucket for BigQuery export data
bucket = 'seattle-fire-and-crime'
spark.conf.set('temporaryGcsBucket', bucket)

def main():
    fire_file_path = 'gs://seattle-fire-and-crime/fire_data.csv'
    crime_file_path = 'gs://seattle-fire-and-crime/crime_data.csv'
    neighborhood_file_path = 'gs://seattle-fire-and-crime/Neighborhood_Map_Atlas_Neighborhoods.geojson'

    fire_schema = StructType([
        StructField('address', StringType(), False),
        StructField('type', StringType(), False),
        StructField('datetime', TimestampNTZType(), False),
        StructField('latitude', DecimalType(), False),
        StructField('longitude', DecimalType(), False),
        StructField('report_location', StringType(), False),
        StructField('incident_number', StringType(), False)
    ])

    crime_schema = StructType([
        StructField('report_number', StringType(), False),
        StructField('offense_id', StringType(), False),
        StructField('offense_start_datetime', TimestampNTZType(), False),
        StructField('offense_end_datetime', TimestampNTZType(), False),
        StructField('report_datetime', TimestampNTZType(), False),
        StructField('group_a_b', StringType(), False),
        StructField('crime_against_category', StringType(), False),
        StructField('offense_parent_group', StringType(), False),
        StructField('offense', StringType(), False),
        StructField('offense_code', StringType(), False),
        StructField('precinct', StringType(), False),
        StructField('sector', StringType(), False),
        StructField('beat', StringType(), False),
        StructField('mcpp', StringType(), False),
        StructField('_100_block_address', StringType(), False),
        StructField('longitude', DecimalType(), False),
        StructField('latitude', DecimalType(), False),
    ])

    fire_data = spark.read.csv(fire_file_path, header = True, schema = fire_schema)
    crime_data = spark.read.csv(crime_file_path, header = True, schema = crime_schema)

    neighborhood_data = sedona.read.format('geojson').option('multiLine', 'true').load(neighborhood_file_path) \
            .selectExpr('explode(features) as features') \
            .select('features.*') \
            .withColumn('district', expr("properties['L_HOOD']").cast(StringType())) \
            .withColumn('neighborhood', expr("properties['S_HOOD']").cast(StringType())) \
            .withColumn('geometry', ST_AsText(ST_GeomFromGeoJSON(col('geometry').cast('string')))) \
            .drop('properties') \
            .drop('type')
    
    neighborhood_data = neighborhood_data.select(
        col('district').cast(StringType()).alias('district'),
        col('neighborhood').cast(StringType()).alias('neighborhood'),
        col('geometry').cast(StringType()).alias('geometry')
)
    
    neighborhood_data.show(5)
    
    dfs = {'fire_data': fire_data, 
            'crime_data': crime_data, 
            'neighborhood_data': neighborhood_data}

    # Save the data to BigQuery (overwriting for now before incremental batch load is implemented)
    for name, df in dfs.items():
        df.write.format('bigquery') \
            .option('table', f'seattle_dataset.{name}') \
            .mode('overwrite') \
            .save()

    spark.stop()

if __name__ == '__main__':
    main()