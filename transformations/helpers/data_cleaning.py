from sedona.spark import *
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
import pyspark.sql.functions as F

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

def add_neighborhood(df: DataFrame, neighb_info: DataFrame) -> DataFrame:
    point_df = df.withColumn('point', ST_Point(df.longitude, df.latitude))
    neighb_df = point_df.alias('point_df') \
        .join(F.broadcast(neighb_info).alias('neighb_info'), ST_Within(point_df.point, neighb_info.geometry), 'left') 

    return neighb_df

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
