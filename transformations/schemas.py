from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampNTZType, BinaryType
from sedona.sql.types import GeometryType

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

# combining crime.offense_start_datetime into datetime, longitude/latitude, address/_100_block_address
all_incidents_schema = StructType([
    StructField('incident_type', StringType(), False),
    StructField('address', StringType(), False),
    StructField('type', StringType(), False),
    StructField('report_location', StringType(), False),
    StructField('incident_number', StringType(), False),
    StructField('report_number', StringType(), False),
    StructField('offense_id', StringType(), False),
    StructField('datetime', TimestampNTZType(), False),
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
    StructField('longitude', DecimalType(25, 20), False),
    StructField('latitude', DecimalType(25, 20), False),
    StructField('geometry', GeometryType(), False),
    StructField('district', StringType(), False),
    StructField('neighborhod', StringType(), False)
])

dim_incident_type_schema = StructType([
    StructField('incident_type_id', IntegerType(), False),
    StructField('incident_type_name', StringType(), False)
])

dim_neighborhood_schema = StructType([
    StructField('neighborhood_id', IntegerType(), False),
    StructField('district_name', StringType(), False),
    StructField('neighborhood_name', StringType(), False),
    StructField('neighborhood_wkt', StringType(), False)
])