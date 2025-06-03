from interfaces import SinkData
from pyspark.sql import DataFrame 
from typing import Optional
from pyspark.sql.functions import lit,concat,col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType , TimestampType , DoubleType , FloatType, DateType

class ModelDatawahouseGoldNYBike:
    def get_df_fact_trip(self, df: DataFrame, config: dict):
        df_new = df.select(col('trip_uuid').alias('fact_id_uuid'),
                          col('enr_rideable_type_id').alias('dim_rideable_fk'),
                          'start_at',
                          'stop_at',
                          'enr_trip_duration'
                          )
        config['dbtable'] = 'warehouse.gold.fact_trip'
        return df_new, config

    def get_df_location(self, df: DataFrame, config: dict):
        df_new = df.select(col('trip_uuid').alias('dim_location_uuid_id'),
                          "start_station_id",
                          "start_station_name",
                          "start_station_latitude",
                          "start_station_longitude",
                          "end_station_id",
                          "end_station_name",
                          "end_station_latitude",
                          "end_station_longitude",
                          "start_at"
        )
        config['dbtable'] = 'warehouse.gold.dim_location'
        return df_new, config

    def get_df_dim_customer(self, df: DataFrame, config: dict):
        df_new = df.select(col('trip_uuid').alias('dim_customer_uuid'),
                           "user_type",
                           "enr_user_type",
                           "gender",
                           "enr_gender",
                           "customer_year_birth"
        )
        config['dbtable'] = 'warehouse.gold.dim_customer'
        return df_new, config
    
    def get_df_dim_rideable(self, df: DataFrame, config: dict):
        df_new = df.select(
            "enr_rideable_type_id",
	        "enr_rideable_type").distinct()
        config['dbtable'] = 'warehouse.gold.dim_rideable'
        return df_new, config


class ModelDatawahouseNYBikeV2:
    def _select_and_update_config(self, df, columns, table_name, config):
        config['dbtable'] = table_name
        return df.select(columns), config

    def get_df_dim_date(self, df: DataFrame, config: dict):
        columns = ['trip_id', 'year', 'quarter', concat('year', lit('Q'), 'quarter').alias('quaster_name'),
                   'month', 'month_name', 'day', 'weekday', 'weekdayname', 'start_at','stop_at']
        return self._select_and_update_config(df, columns, 'dim_date_nybike', config)

    def get_df_dim_location(self, df: DataFrame, config: dict):
        columns = ['trip_id', 'year', 'month', 'start_staion_name',
                   'start_staion_id', 'start_staion_latitude', 'start_staion_longitude',
                   'end_station_name', 'end_station_id', 'end_station_latitude', 'end_station_longitude']
        return self._select_and_update_config(df, columns, 'dim_station_nybkide', config)

    def get_df_dim_customer(self, df: DataFrame, config: dict):
        columns = ['trip_id', 'year', 'month', 'rideable_type',
                   'customer_type', 'customer_gender', 'year_birth', 'trip_duration']
        return self._select_and_update_config(df, columns, 'fact_nybike', config)
    
    def get_df_dim_rideable(self, df: DataFrame, config: dict):
        columns = ['trip_id', 'year', 'month', 'rideable_type',
                   'customer_type', 'customer_gender', 'year_birth', 'trip_duration']
        return self._select_and_update_config(df, columns, 'fact_nybike', config)
    
    def get_df_dim_fact(self, df: DataFrame, config: dict):
        columns = ['fact_id_uuid', 'dim_rideable_fk',
                   'start_at', 'stopt_at', 'enr_trip_duration']
        return self._select_and_update_config(df, columns, 'fact_nybike', config)



# Define the schema as a StructType
source_old_schema_ny_bike = StructType([
    StructField("tripduration", IntegerType(), nullable=False),  # Trip duration in seconds
    StructField("starttime", TimestampType(), nullable=True),    # Start time of the trip
    StructField("stoptime", TimestampType(), nullable=True),     # Stop time of the trip
    StructField("start station id", IntegerType(), nullable=True),  # Start station ID
    StructField("start station name", StringType(), nullable=True),  # Start station name
    StructField("start station latitude", DoubleType(), nullable=True),  # Start station latitude
    StructField("start station longitude", DoubleType(), nullable=True),  # Start station longitude
    StructField("end station id", IntegerType(), nullable=True),  # End station ID
    StructField("end station name", StringType(), nullable=True),  # End station name
    StructField("end station latitude", DoubleType(), nullable=True),  # End station latitude
    StructField("end station longitude", DoubleType(), nullable=True),  # End station longitude
    StructField("bikeid", IntegerType(), nullable=True),  # Bike ID
    StructField("usertype", StringType(), nullable=True),  # User type (e.g., Customer or Subscriber)
    StructField("birth year", IntegerType(), nullable=True),  # Birth year of the user
    StructField("gender", IntegerType(), nullable=True)  # Gender (e.g., 0 = unknown, 1 = male, 2 = female)
])


# Define the schema as a StructType
source_actual_schema_ny_bike = StructType([
    StructField("ride_id", StringType(), nullable=False),         # Unique identifier for the ride
    StructField("rideable_type", StringType(), nullable=True),    # Type of bike (e.g., classic_bike, electric_bike)
    StructField("started_at", TimestampType(), nullable=True),    # Start time of the ride
    StructField("ended_at", TimestampType(), nullable=True),      # End time of the ride
    StructField("start_station_name", StringType(), nullable=True),  # Name of the start station
    StructField("start_station_id", StringType(), nullable=True),  # ID of the start station
    StructField("end_station_name", StringType(), nullable=True),   # Name of the end station
    StructField("end_station_id", StringType(), nullable=True),     # ID of the end station
    StructField("start_lat", DoubleType(), nullable=True),          # Latitude of the start location
    StructField("start_lng", DoubleType(), nullable=True),          # Longitude of the start location
    StructField("end_lat", DoubleType(), nullable=True),            # Latitude of the end location
    StructField("end_lng", DoubleType(), nullable=True),            # Longitude of the end location
    StructField("member_casual", StringType(), nullable=True)       # User type (e.g., member or casual rider)
])



# Define the schema
bronze_schema_ny_bike = StructType([
    StructField("dw_period_tag", StringType(), nullable=True),
    StructField("ride_id", StringType(), nullable=True),
    StructField("start_station_id", StringType(), nullable=True),
    StructField("start_station_name", StringType(), nullable=True),
    StructField("start_station_latitude", StringType(), nullable=True),
    StructField("start_station_longitude", StringType(), nullable=True),
    StructField("end_station_id", StringType(), nullable=True),
    StructField("end_station_name", StringType(), nullable=True),
    StructField("end_station_latitude", StringType(), nullable=True),
    StructField("end_station_longitude", StringType(), nullable=True),
    StructField("bike_id", StringType(), nullable=True),
    StructField("user_type", StringType(), nullable=True),
    StructField("gender", StringType(), nullable=True),
    StructField("customer_year_birth", StringType(), nullable=True),
    StructField("rideable_type", StringType(), nullable=True),
    StructField("start_at", StringType(), nullable=True),
    StructField("stop_at", StringType(), nullable=True),
    StructField("trip_duration", StringType(), nullable=True)
])



silver_schema_ny_bike = StructType([
    StructField("trip_uuid", StringType(), nullable=True),
    StructField("dw_period_tag", StringType(), nullable=True),
    StructField("start_station_id", StringType(), nullable=True),
    StructField("start_station_name", StringType(), nullable=True),
    StructField("start_station_latitude", StringType(), nullable=True),
    StructField("start_station_longitude", StringType(), nullable=True),
    StructField("end_station_id", StringType(), nullable=True),
    StructField("end_station_name", StringType(), nullable=True),
    StructField("end_station_latitude", StringType(), nullable=True),
    StructField("end_station_longitude", StringType(), nullable=True),
    StructField("bike_id", StringType(), nullable=True),
    StructField("gender", IntegerType(), nullable=True),
    StructField("enr_gender", StringType(), nullable=True),
    StructField("customer_year_birth", StringType(), nullable=True),
    StructField("rideable_type", StringType(), nullable=True),
    StructField("enr_rideable_type", StringType(), nullable=True),
    StructField("enr_rideable_type_id", IntegerType(), nullable=True),
    StructField("start_at", TimestampType(), nullable=True),
    StructField("stop_at", TimestampType(), nullable=True),
    StructField("trip_duration", DoubleType(), nullable=True),
    StructField("enr_trip_duration", DoubleType(), nullable=True),
    StructField("user_type", StringType(), nullable=True),
    StructField("enr_user_type", StringType(), nullable=True),
    StructField("enr_year", IntegerType(), nullable=True),
    StructField("enr_quarter", IntegerType(), nullable=True),
    StructField("enr_quarter_name", StringType(), nullable=True),
    StructField("enr_month", IntegerType(), nullable=True),
    StructField("enr_month_name", StringType(), nullable=True),
    StructField("enr_day", IntegerType(), nullable=True),
    StructField("enr_weekday", IntegerType(), nullable=True),
    StructField("enr_weekday_name", StringType(), nullable=True)
])