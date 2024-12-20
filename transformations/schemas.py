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
    StructField('incident_id', IntegerType(), False),
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
    StructField('neighborhood', StringType(), False)
])

fact_incident_schema = StructType([
    StructField('incident_id', IntegerType(), False),
    StructField('neighborhood_id', IntegerType(), False),
    StructField('date_id', IntegerType(), False),
    StructField('incident_type_id', IntegerType(), False),
    StructField('address', StringType(), False),
    StructField('type', StringType(), False),
    StructField('report_location', StringType(), False),
    StructField('incident_number', StringType(), False),
    StructField('report_number', StringType(), False),
    StructField('offense_id', StringType(), False),
    StructField('group_a_b', StringType(), False),
    StructField('crime_against_category', StringType(), False),
    StructField('offense_parent_group', StringType(), False),
    StructField('offense', StringType(), False),
    StructField('offense_code', StringType(), False),
    StructField('precinct', StringType(), False),
    StructField('sector', StringType(), False),
    StructField('beat', StringType(), False),
    StructField('mcpp', StringType(), False),
    StructField('offense_end_datetime', TimestampNTZType(), False),     # Crime Only offense end datetime
    StructField('report_datetime', TimestampNTZType(), False),          # Crime Only reported datetime
    StructField('longitude', DecimalType(25, 20), False),
    StructField('latitude', DecimalType(25, 20), False)
])

dim_incident_type_schema = StructType([
    StructField('incident_type_id', IntegerType(), False),
    StructField('incident_type', StringType(), False)
])

dim_neighborhood_schema = StructType([
    StructField('neighborhood_id', IntegerType(), False),
    StructField('district', StringType(), False),
    StructField('neighborhood', StringType(), False),
    StructField('geometry', StringType(), False)
])

dim_date_schema = StructType([
    StructField('date_id', IntegerType(), False),
    StructField('datetime', TimestampNTZType(), False),                 # Fire datetime and Crime offense start datetime
    StructField('year', IntegerType(), False),                          # Year
    StructField('month', IntegerType(), False),                         # Month (1 to 12)
    StructField('day', IntegerType(), False),                           # Day of the month (1 to 31)
    StructField('hour', IntegerType(), False),                          # Hour (0 to 23)
    StructField('minute', IntegerType(), False),                        # Minute (0 to 59)
    StructField('second', IntegerType(), False),                        # Second (0 to 59)
    StructField('day_of_week', IntegerType(), False),                   # Day of the week (1 = Sunday, 7 = Saturday)
    StructField('week_of_year', IntegerType(), False),                  # Week of the year (1 to 52)
    StructField('weekday_name', StringType(), False),                   # Full weekday name (e.g., Monday)
    StructField('month_name', StringType(), False),                     # Full month name (e.g., January)
    StructField('quarter', IntegerType(), False)                        # Quarter (1, 2, 3, or 4)
])