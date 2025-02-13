from interfaces import SinkData
from pyspark.sql import DataFrame 
from typing import Optional
from pyspark.sql.functions import lit,concat

class ModelDatawahouseNYBike:
    def get_df_DIMDATE(self, df: DataFrame, config: dict):
        df_new = df.select('DATE_ID',
                          'YEAR',
                          'QUARTER',
                          concat('YEAR', lit('Q'), 'QUARTER').alias('QUARTER_NAME'),
                          'MONTH',
                          'MONTH_NAME',
                          'DAY',
                          'WEEKDAY',
                          'WEEKDAYNAME',
                          'START_AT',
                          'STOP_AT')
        config['dbtable'] = 'DIM_DATE_NYBIKE'
        return df_new, config

    def get_df_DIM_STATION_NYBIKE(self, df: DataFrame, config: dict):
        df_new = df.select('TRIP_STATION_NYBIKE_ID',
                          'YEAR',
                          'MONTH',
                          'START_STATION_NAME',
                          'START_STATION_ID',
                          'START_STATION_LATITUDE',
                          'START_STATION_LONGITUDE',
                          'END_STATION_NAME',
                          'END_STATION_ID',
                          'END_STATION_LATITUDE',
                          'END_STATION_LONGITUDE')
        config['dbtable'] = 'DIM_STATION_NYBIKE'
        return df_new, config

    def get_df_DIM_FACT(self, df: DataFrame, config: dict):
        df_new = df.select('FACT_ID',
                          df['TRIP_STATION_NYBIKE_ID'].alias('TRIP_STATION_NYBIKE_ID_FK'),
                          df['DATE_ID'].alias('DATE_ID_FK'),
                          'YEAR',
                          'MONTH',
                          'RIDEABLE_TYPE',
                          'CUSTOMER_TYPE',
                          'CUSTOMER_GENDER',
                          'CUSTOMER_YEAR_BIRTH',
                          'TRIP_DURATION')
        config['dbtable'] = 'FACT_NYBIKE'
        return df_new, config

class ModelDatawahouseNYBikeV2:
    def _select_and_update_config(self, df, columns, table_name, config):
        config['dbtable'] = table_name
        return df.select(columns), config

    def get_df_DIMDATE(self, df: DataFrame, config: dict):
        columns = ['DATE_ID', 'YEAR', 'QUARTER', concat('YEAR', lit('Q'), 'QUARTER').alias('QUARTER_NAME'),
                   'MONTH', 'MONTH_NAME', 'DAY', 'WEEKDAY', 'WEEKDAYNAME', 'START_AT','STOP_AT']
        return self._select_and_update_config(df, columns, 'DIM_DATE_NYBIKE', config)

    def get_df_DIM_STATION_NYBIKE(self, df: DataFrame, config: dict):
        columns = ['TRIP_STATION_NYBIKE_ID', 'YEAR', 'MONTH', 'START_STATION_NAME',
                   'START_STATION_ID', 'START_STATION_LATITUDE', 'START_STATION_LONGITUDE',
                   'END_STATION_NAME', 'END_STATION_ID', 'END_STATION_LATITUDE', 'END_STATION_LONGITUDE']
        return self._select_and_update_config(df, columns, 'DIM_STATION_NYBIKE', config)

    def get_df_DIM_FACT(self, df: DataFrame, config: dict):
        columns = ['FACT_ID', df['TRIP_STATION_NYBIKE_ID'].alias('TRIP_STATION_NYBIKE_ID_FK'),
                   df['DATE_ID'].alias('DATE_ID_FK'), 'YEAR', 'MONTH', 'RIDEABLE_TYPE',
                   'CUSTOMER_TYPE', 'CUSTOMER_GENDER', 'CUSTOMER_YEAR_BIRTH', 'TRIP_DURATION']
        return self._select_and_update_config(df, columns, 'FACT_NYBIKE', config)