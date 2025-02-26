from interfaces import DataTransformer
from pyspark.sql import DataFrame
from pyspark.sql.functions import when,col,lit,hash,datediff,xxhash64,year,quarter,dayofmonth,month,dayofweek,date_format,concat
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
        if'gender' in df.columns:
            df= df.withColumn("enr_gender",
                                 when(col('gender') == 1,"Male")
                                 .when(col('gender') == 2,'Female')
                                 .otherwise('Unknown'))
        else:
            df= df.withColumn("enr_gender",lit("Unknown"))
        df.drop('gender')
        return df
    
class TransformCustomerTypeFormat(DataTransformer):
    def run(self,df:DataFrame, config:Optional[dict]) -> DataFrame:
        if'user_type' in df.columns:
            return df.withColumn("customer_type",
                                 when(df['user_type'] == 'Subscriber',"Member")
                                 .otherwise('Customer')).drop('user_type')
        return  df
    


class AddColumnDiffTime(DataTransformer):
    def run(self,df:DataFrame,config:Optional[dict]) -> DataFrame:
        return df.withColumn(config['column_result'],
                             when(col(config['column_result']) == None , datediff(col(config['column_greather']),col(config['colmun_lesser'])))
                             )
    
class AddColumnIDs(DataTransformer):
    def run(self,df:DataFrame,config:Optional[dict]) -> DataFrame:
        for colum_name, value in config['ids_columns'].items():
            df = df.withColumn(value, xxhash64(*config['column_to_hash']))
        return df

class AddRideType(DataTransformer):
    def run(self,df:DataFrame,config:Optional[dict]) -> DataFrame:
        # print(df.columns)
        if 'rideable_type' not in df.columns :
            return df.withColumn('rideable_type',lit('classic_bike'))
        return df

class  AddDimensionsForTimes(DataTransformer):
    def run(self,df:DataFrame,config:Optional[dict]):
        return df.withColumn('year', year(config['datetime_column'])) \
             .withColumn('month', month(config['datetime_column'])) \
             .withColumn('quarter', quarter(config['datetime_column'])) \
             .withColumn('quarter_name', concat(col('year'), lit('Q'), col('quarter'))) \
             .withColumn('day', dayofmonth(config['datetime_column'])) \
             .withColumn('weekday', dayofweek(config['datetime_column'])) \
             .withColumn('month_name', date_format(config['datetime_column'],'MMMM')) \
             .withColumn('weekday_name', date_format(config['datetime_column'],'EEEE'))
        # return df
class AddColumnWithLiteralValue(DataTransformer):
    def run(self,df:DataFrame,config:Optional[dict]):
        print(config['column_to_add']['column_name'])
        print(config['column_to_add']['column_value'])
        df = df.withColumn(config['column_to_add']['column_name'],lit(config['column_to_add']['column_value']))
        # df = df.withColumn('dw_period_tag',lit('2013'))
        df.show()
        return df

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
    ADD_COLUMN_DIFF_TIME ='add_trip_duration'
    TRANSFORM_CUSTOMER_COLUMN ='transform_customer_column'
    ADD_COLUMN_IDS='add_column_ids'
    ADD_DIMENSIONS_TIME='add_column_time'
    ADD_COLUMN_WITH_LITERAL_VALUE='add_column_with_literal_value'
    
    @property
    def get_data_tranformer(self)->DataTransformer:
        return {
            self.RENAME_COLUMNS:RenameColumn(),
            self.DROP_COLUMNS:DropColumns(),
            self.GENDER_TRANSFORMER_OR_ADD:TransformCustomerGenderFormat(),
            self.ADD_COLUMN_DIFF_TIME:AddColumnDiffTime(),
            self.ADD_BIKE_TYPE : AddRideType(),
            self.ADD_COLUMN_IDS : AddColumnIDs(),
            self.TRANSFORM_CUSTOMER_COLUMN:TransformCustomerTypeFormat(),
            self.ADD_DIMENSIONS_TIME:AddDimensionsForTimes(),
            self.ADD_COLUMN_WITH_LITERAL_VALUE:AddColumnWithLiteralValue()
        }[self]

class DataTransformerObject(BaseModel):
    transformer : FactoryDataTransformer
    config : dict

def runner_transformer_data(catalogue_data_transformer:list[DataTransformerObject], data:DataFrame):
    for element in catalogue_data_transformer:
        transformer = element.transformer.get_data_tranformer
        data = transformer.run(data,element.config)
    return data