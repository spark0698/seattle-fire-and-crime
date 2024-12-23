from sedona.spark import *
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from typing import Optional

def load_data(spark: SparkSession, sedona: SedonaContext, filename: str, schema_name: Optional[StructType] = None, infer_schema: bool = True) -> DataFrame:
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

def write_to_bigquery(dfs: dict, m: str):
    # Save the data to BigQuery (overwriting for now before incremental batch load is implemented)
    for name, df in dfs.items():
        df.write.format('bigquery') \
            .option('writeMethod', 'direct') \
            .mode(m) \
            .save(f'seattle_dataset.{name}')
        