from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampNTZType, BinaryType

fire_schema = StructType([
    StructField('address', StringType(), False),
    StructField('type', StringType(), False),
    StructField('datetime', TimestampNTZType(), False),
    StructField('latitude', DecimalType(25, 20), False),
    StructField('longitude', DecimalType(25, 20), False),
    StructField('report_location', StringType(), False),
    StructField('incident_number', StringType(), False)
])

crime_schema = StructType([
    StructField('report_number', StringType(), False),
    StructField('offense_id', StringType(), False),
    StructField('offense_start_datetime', TimestampNTZType(), False),
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
    StructField('longitude', DecimalType(25, 20), False),
    StructField('latitude', DecimalType(25, 20), False),
    StructField('offense_end_datetime', TimestampNTZType(), False)
])

dim_incident_type_schema = StructType([
    StructField('incident_type_id', IntegerType(), False),
    StructField('incident_type_name', StringType(), False)
])