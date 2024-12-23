from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampNTZType, BinaryType
from sedona.sql.types import GeometryType

fire_schema = StructType([
    StructField('address', StringType()),
    StructField('type', StringType()),
    StructField('datetime', TimestampNTZType()),
    StructField('latitude', DecimalType(25, 20)),
    StructField('longitude', DecimalType(25, 20)),
    StructField('report_location', StringType()),
    StructField('incident_number', StringType())
])

crime_schema = StructType([
    StructField('report_number', StringType()),
    StructField('offense_id', StringType()),
    StructField('offense_start_datetime', TimestampNTZType()),
    StructField('report_datetime', TimestampNTZType()),
    StructField('group_a_b', StringType()),
    StructField('crime_against_category', StringType()),
    StructField('offense_parent_group', StringType()),
    StructField('offense', StringType()),
    StructField('offense_code', StringType()),
    StructField('precinct', StringType()),
    StructField('sector', StringType()),
    StructField('beat', StringType()),
    StructField('mcpp', StringType()),
    StructField('_100_block_address', StringType()),
    StructField('longitude', DecimalType(25, 20)),
    StructField('latitude', DecimalType(25, 20)),
    StructField('offense_end_datetime', TimestampNTZType())
])

# combining crime.offense_start_datetime into datetime, longitude/latitude, address/_100_block_address
all_incidents_schema = StructType([
    StructField('incident_id', IntegerType()),
    StructField('incident_type', StringType()),
    StructField('address', StringType()),
    StructField('type', StringType()),
    StructField('report_location', StringType()),
    StructField('incident_number', StringType()),
    StructField('report_number', StringType()),
    StructField('offense_id', StringType()),
    StructField('datetime', TimestampNTZType()),
    StructField('offense_end_datetime', TimestampNTZType()),
    StructField('report_datetime', TimestampNTZType()),
    StructField('group_a_b', StringType()),
    StructField('crime_against_category', StringType()),
    StructField('offense_parent_group', StringType()),
    StructField('offense', StringType()),
    StructField('offense_code', StringType()),
    StructField('precinct', StringType()),
    StructField('sector', StringType()),
    StructField('beat', StringType()),
    StructField('mcpp', StringType()),
    StructField('longitude', DecimalType(25, 20)),
    StructField('latitude', DecimalType(25, 20)),
    StructField('geometry', GeometryType()),
    StructField('district', StringType()),
    StructField('neighborhood', StringType())
])

fact_incident_schema = StructType([
    StructField('incident_id', IntegerType()),
    StructField('neighborhood_id', IntegerType()),
    StructField('date_id', IntegerType()),
    StructField('incident_type_id', IntegerType()),
    StructField('crime_detail_id', IntegerType()),
    StructField('address', StringType()),
    StructField('type', StringType()),
    StructField('report_location', StringType()),
    StructField('incident_number', StringType()),
    StructField('longitude', DecimalType(25, 20)),
    StructField('latitude', DecimalType(25, 20))
])

dim_incident_type_schema = StructType([
    StructField('incident_type_id', IntegerType()),
    StructField('incident_type', StringType())
])

dim_neighborhood_schema = StructType([
    StructField('neighborhood_id', IntegerType()),
    StructField('district', StringType()),
    StructField('neighborhood', StringType()),
    StructField('geometry', StringType())
])

dim_date_schema = StructType([
    StructField('date_id', IntegerType()),
    StructField('datetime', TimestampNTZType()),                 # Fire datetime and Crime offense start datetime
    StructField('year', IntegerType()),                          # Year
    StructField('month', IntegerType()),                         # Month (1 to 12)
    StructField('day', IntegerType()),                           # Day of the month (1 to 31)
    StructField('hour', IntegerType()),                          # Hour (0 to 23)
    StructField('minute', IntegerType()),                        # Minute (0 to 59)
    StructField('second', IntegerType()),                        # Second (0 to 59)
    StructField('day_of_week', IntegerType()),                   # Day of the week (1 = Sunday, 7 = Saturday)
    StructField('week_of_year', IntegerType()),                  # Week of the year (1 to 52)
    StructField('weekday_name', StringType()),                   # Full weekday name (e.g., Monday)
    StructField('month_name', StringType()),                     # Full month name (e.g., January)
    StructField('quarter', IntegerType())                        # Quarter (1, 2, 3, or 4)
])

dim_crime_details_schema = StructType([
    StructField('crime_detail_id', IntegerType()),
    StructField('report_number', StringType()),
    StructField('offense_id', StringType()),
    StructField('offense_end_datetime', TimestampNTZType()),
    StructField('report_datetime', TimestampNTZType()),
    StructField('group_a_b', StringType()),
    StructField('crime_against_category', StringType()),
    StructField('offense_parent_group', StringType()),
    StructField('offense', StringType()),
    StructField('offense_code', StringType()),
    StructField('precinct', StringType()),
    StructField('sector', StringType()),
    StructField('beat', StringType()),
    StructField('mcpp', StringType())
])

dim_fire_details_schema = StructType([
    StructField('fire_detail_id', IntegerType()),
    StructField('type', StringType()),
    StructField('report_location', StringType()),
    StructField('incident_number', StringType())
])