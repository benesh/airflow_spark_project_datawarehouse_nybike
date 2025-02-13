from interfaces import DataTransformer
from pyspark.sql import DataFrame
from pyspark.sql.functions import when,col,lit,hash,datediff,xxhash64,year,quarter,dayofmonth,month,dayofweek,date_format
# from utils import get_distnace
from enum import Enum
from pydantic import BaseModel
from typing import Optional

class DropColumns(DataTransformer):
    def run(sefl,df:DataFrame,config:Optional[dict]) -> DataFrame:
        for column in config['columns_to_delete']:
            df = df.drop(column)
        return df

class RenameColumn(DataTransformer) :
    # config['column_to_rename'] it should be a dict of column present and the new name of the column 
    def run(self,df:DataFrame,config:Optional[dict]) -> DataFrame:
        df = df.withColumnsRenamed(config['columns_to_rename'])
        return df

"""
It should be run over a DataFrame that is final on naming column
"""
    
class TransformCustomerGenderFormat(DataTransformer):
    def run(self,df:DataFrame, config:Optional[dict]) -> DataFrame:
        if'GENDER' in df.columns:
            return df.withColumn("CUSTOMER_GENDER",
                                 when(col('GENDER') == 1,"Male")
                                 .when(col('GENDER') == 2,'Female')
                                 .otherwise('Unknown')).drop('GENDER')
        return  df.withColumn("CUSTOMER_GENDER",lit("Unknown"))
    
class TransformCustomerTypeFormat(DataTransformer):
    def run(self,df:DataFrame, config:Optional[dict]) -> DataFrame:
        if'USER_TYPE' in df.columns:
            return df.withColumn("CUSTOMER_TYPE",
                                 when(df['USER_TYPE'] == 'Subscriber',"Member")
                                 .otherwise('Customer')).drop('USER_TYPE')
        return  df

class AddTripDuration(DataTransformer):
    def run(self,df:DataFrame,config:Optional[dict]) -> DataFrame:
        if 'TRIP_DURATION' in df.columns:
            return df.withColumn('TRIP_DURATION',datediff(col('STOP_AT'),col('START_AT')))
        return df
    
class AddColumnIDs(DataTransformer):
    def run(self,df:DataFrame,config:Optional[dict]) -> DataFrame:
        for colum_name, value in config['ids_columns'].items():
            df = df.withColumn(value, xxhash64(*config['column_to_hash']))
        return df

class AddRideType(DataTransformer):
    def run(self,df:DataFrame,config:Optional[dict]) -> DataFrame:
        if 'RIDEBLE_TYPE' not in df.columns :
            return df.withColumn('RIDEABLE_TYPE',lit('classic_bike'))
        return df

class  AddDimensionsForTimes(DataTransformer):
    def run(self,df:DataFrame,config:Optional[dict]):
        return df.withColumn('YEAR', year(config['datetime_column'])) \
             .withColumn('MONTH', month(config['datetime_column'])) \
             .withColumn('QUARTER', quarter(config['datetime_column'])) \
             .withColumn('DAY', dayofmonth(config['datetime_column'])) \
             .withColumn('WEEKDAY', dayofweek(config['datetime_column'])) \
             .withColumn('MONTH_NAME', date_format(config['datetime_column'], 'MMMM')) \
             .withColumn('WEEKDAYNAME', date_format(config['datetime_column'], 'EEEE'))
        # return df


# class EnrichWithDistance(DataTransformer):
#     def run(self,df:DataFrame,config:Optional[dict]) -> DataFrame :
#         df = df.withColumn('TRIP_DISTANCE', \
#                             get_distnace((df.START_STATION_LATITUDE,df.START_STATION_LONGITUDE),(df.END_STATION_LATITUDE,df.END_STATION_LONGITUDE)))
#         return df

class FactoryDataTransformer(Enum):
    RENAME_COLUMNS='rename_column'
    DROP_COLUMNS ='drop_columns'
    GENDER_TRANSFORMER_OR_ADD = 'gender_transformer_or_add'
    ADD_BIKE_TYPE = 'add_bike_type'
    ADD_TRIP_DURATION ='add_trip_duration'
    TRANSFORM_CUSTOMER_COLUMN ='transform_customer_column'
    ADD_COLUMN_IDS='add_column_ids'
    ADD_DIMENSIONS_TIME='add_column_time'
    
    @property
    def get_data_tranformer(self)->DataTransformer:
        return {
            self.RENAME_COLUMNS:RenameColumn(),
            self.DROP_COLUMNS:DropColumns(),
            self.GENDER_TRANSFORMER_OR_ADD:TransformCustomerGenderFormat(),
            self.ADD_TRIP_DURATION:AddTripDuration(),
            self.ADD_BIKE_TYPE : AddRideType(),
            self.ADD_COLUMN_IDS : AddColumnIDs(),
            self.TRANSFORM_CUSTOMER_COLUMN:TransformCustomerTypeFormat(),
            self.ADD_DIMENSIONS_TIME:AddDimensionsForTimes()
        }[self]

class DataTransformerObject(BaseModel):
    transformer : FactoryDataTransformer
    config : dict

def runner_transformer_data(catalogue_data_transformer:list[DataTransformerObject], data:DataFrame):
    for element in catalogue_data_transformer:
        transformer = element.transformer.get_data_tranformer
        data = transformer.run(data,element.config)
    return data