from interfaces import SinkData
from pyspark.sql import DataFrame 
from typing import Optional
from pyspark.sql.functions import lit,concat
from pyspark.sql.types import StructType, StructField, StringType, IntegerType , TimestampType , DoubleType , FloatType, DateType

class ModelDatawahouseNYBike:
    def get_df_DIMDATE(self, df: DataFrame, config: dict):
        df_new = df.select('trip_id',
                          'year',
                          'quarter',
                          concat('year', lit('Q'), 'quarter').alias('quarter_name'),
                          'month',
                          'month_name',
                          'day',
                          'weekday',
                          'weekdayname',
                          'start_at',
                          'stop_at')
        config['dbtable'] = 'dim_date_nybike'
        return df_new, config

    def get_df_DIM_STATION_NYBIKE(self, df: DataFrame, config: dict):
        df_new = df.select('trip_id',
                          'year',
                          'month',
                          'start_station_name',
                          'start_station_id',
                          'start_station_latitude',
                          'start_station_longitude',
                          'end_station_name',
                          'end_station_ID',
                          'end_station_latitude',
                          'end_station_longitude')
        config['dbtable'] = 'dim_station_nybike'
        return df_new, config

    def get_df_DIM_FACT(self, df: DataFrame, config: dict):
        df_new = df.select('trip_id',
                          'year',
                          'month',
                          'rideable_type',
                          'customer_type',
                          'cutomer_gender',
                          'year_birth',
                          'trip_duration')
        config['dbtable'] = 'fact_nybike'
        return df_new, config

class ModelDatawahouseNYBikeV2:
    def _select_and_update_config(self, df, columns, table_name, config):
        config['dbtable'] = table_name
        return df.select(columns), config

    def get_df_DIMDATE(self, df: DataFrame, config: dict):
        columns = ['trip_id', 'year', 'quarter', concat('year', lit('Q'), 'quarter').alias('quaster_name'),
                   'month', 'month_name', 'day', 'weekday', 'weekdayname', 'start_at','stop_at']
        return self._select_and_update_config(df, columns, 'dim_date_nybike', config)

    def get_df_DIM_STATION_NYBIKE(self, df: DataFrame, config: dict):
        columns = ['trip_id', 'year', 'month', 'start_staion_name',
                   'start_staion_id', 'start_staion_latitude', 'start_staion_longitude',
                   'end_station_name', 'end_station_id', 'end_station_latitude', 'end_station_longitude']
        return self._select_and_update_config(df, columns, 'dim_station_nybkide', config)

    def get_df_DIM_FACT(self, df: DataFrame, config: dict):
        columns = ['trip_id', 'year', 'month', 'rideable_type',
                   'customer_type', 'customer_gender', 'year_birth', 'trip_duration']
        return self._select_and_update_config(df, columns, 'fact_nybike', config)


# Define the schema
bronze_schema_ny_bike = StructType([
    StructField("start_station_id", StringType(), nullable=True),
    StructField("start_station_name", StringType(), nullable=True),
    StructField("start_station_latitude", DoubleType(), nullable=True),
    StructField("start_station_longitude", DoubleType(), nullable=True),
    StructField("end_station_id", StringType(), nullable=True),
    StructField("end_station_name", StringType(), nullable=True),
    StructField("end_station_latitude", DoubleType(), nullable=True),
    StructField("end_station_longitude", DoubleType(), nullable=True),
    StructField("bike_id", IntegerType(), nullable=True),
    StructField("user_type", StringType(), nullable=True),
    StructField("gender", StringType(), nullable=True),
    StructField("customer_year_birth", StringType(), nullable=True),
    StructField("rideable_type", StringType(), nullable=True),
    StructField("start_at", TimestampType(), nullable=True),
    StructField("stop_at", TimestampType(), nullable=True),
    StructField("trip_duration", DoubleType(), nullable=True)
])

sylver_schema_ny_bike = StructType([
    StructField("start_station_id", StringType(), nullable=True),
    StructField("start_station_name", StringType(), nullable=True),
    StructField("start_station_latitude", DoubleType(), nullable=True),
    StructField("start_station_longitude", DoubleType(), nullable=True),
    StructField("end_station_id", StringType(), nullable=True),
    StructField("end_station_name", StringType(), nullable=True),
    StructField("end_station_latitude", DoubleType(), nullable=True),
    StructField("end_station_longitude", DoubleType(), nullable=True),
    StructField("bike_id", IntegerType(), nullable=True),
    StructField("user_type", StringType(), nullable=True),
    StructField("gender", StringType(), nullable=True),
    StructField("enr_gender", StringType(), nullable=True),
    StructField("customer_year_birth", StringType(), nullable=True),
    StructField("rideable_type", StringType(), nullable=True),
    StructField("start_at", TimestampType(), nullable=True),
    StructField("stop_at", TimestampType(), nullable=True),
    StructField("trip_duration", DoubleType(), nullable=True),
    StructField("customer_type", StringType(), nullable=True),
    StructField("quarter", IntegerType(), nullable=True),
    StructField("quarter_name", StringType(), nullable=True),
    StructField("month", IntegerType(), nullable=True),
    StructField("month_name", StringType(), nullable=True),
    StructField("day", IntegerType(), nullable=True),
    StructField("weekday", IntegerType(), nullable=True),
    StructField("weekday_name", StringType(), nullable=True)
])