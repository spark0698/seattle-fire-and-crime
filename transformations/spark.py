from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, hour, minute, unix_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampNTZType

spark = SparkSession.builder.appName('SeattleIncidents') \
    .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar") \
    .getOrCreate()

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

    fire_data = spark.read.csv(fire_file_path, header = True, schema = fire_schema)
    
    # Save the data to BigQuery
    fire_data.write.format('bigquery') \
        .option('table', 'seattle_dataset.seattle_fire') \
        .save()

    spark.stop()

if __name__ == '__main__':
    main()