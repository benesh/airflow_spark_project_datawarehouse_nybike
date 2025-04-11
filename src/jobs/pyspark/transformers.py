from interfaces import DataTransformer
from pyspark.sql import DataFrame
from pyspark.sql.functions import when,col,lit,hash,datediff,xxhash64,year,quarter,dayofmonth,month,dayofweek,date_format,concat,to_timestamp
# from utils import get_distnace
from enum import Enum
from pydantic import BaseModel
from typing import Optional
import uuid

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
        print("Gend transformation initiated")
        if 'gender' in df.columns:
            df= df.withColumn("enr_gender",
                                 when(col('gender') == 1,"Male")
                                 .when(col('gender') == 2,'Female')
                                 .otherwise('Unknown'))
        else:
            df= df.withColumn("enr_gender",lit("Unknown"))
        df=df.drop('gender')
        return df
    
class TransformCustomerTypeFormat(DataTransformer):
    def run(self,df:DataFrame, config:Optional[dict]) -> DataFrame:
        print("Customer type transformation initiated")
        if'user_type' in df.columns:
            return df.withColumn("customer_type",
                                 when(df['user_type'] == 'Subscriber',"Member")
                                 .otherwise('Customer')).drop('user_type')
        return  df


class AddColumnDiffTime(DataTransformer):
    def run(self,df:DataFrame,config:Optional[dict]) -> DataFrame:
        print("Duration calculation initiated")
        return df.withColumn(config['column_result'],
                             when(col(config['column_result']).isNull(),\
                                   col(config['column_greather']).cast('long') - col(config['colmun_lesser']).cast('long'))
                             .otherwise(col(config['column_result']))
                             )
    
class AddColumnIDs(DataTransformer):
    def run(self,df:DataFrame,config:Optional[dict]) -> DataFrame:
        for colum_name, value in config['ids_columns'].items():
            df = df.withColumn(value, xxhash64(*config['column_to_hash']))
        return df

class AddRideType(DataTransformer):
    def run(self,df:DataFrame,config:Optional[dict]) -> DataFrame:
        print('Ride type add column initiate')
        return df.withColumn('rideable_type',when(col('rideable_type').isNull(), lit('classic_bike')).otherwise(col('rideable_type')))

class  AddDimensionsForTimes(DataTransformer):
    def run(self,df:DataFrame,config:Optional[dict]):
        print("Dimensions time column add initiated")
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
        print("Column With value literal initiated")
        df = df.withColumn(config['column_to_add']['column_name'],lit(config['column_to_add']['column_value']))
        return df
    
class CastToDatamodel(DataTransformer):
    def run(self,df:DataFrame,config:Optional[dict]):
        print("Cast To Datamodel initiated")
        data_model_from_df = list(dict(df.dtypes).keys())

        filtered_column = list(filter( lambda x: x not in data_model_from_df ,config['schema'].names ))
        if len(filtered_column) > 0 :
            for column in filtered_column:
                df =  df.withColumn(column, lit(None))

        df = df.select([
            col(field.name).cast(field.dataType).alias(field.name) 
            for field in config['schema'].fields
        ])
        return df
    
class AddUuidToColumnID(DataTransformer):
    def run(self,df:DataFrame,config:Optional[dict]):
        print(f"Add uuid in to th column: {config['column_id']}")
        return df.withColumn(config['column_id'],lit(str(uuid.uuid4())))



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
    CAST_TO_DATAMODEL='cast_to_datamodel'
    AddUuidToColumnID='add_uuid_to_column_id'
    
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
            self.ADD_COLUMN_WITH_LITERAL_VALUE:AddColumnWithLiteralValue(),
            self.CAST_TO_DATAMODEL:CastToDatamodel(),
            self.AddUuidToColumnID:AddUuidToColumnID()
        }[self]

class DataTransformerObject(BaseModel):
    transformer : FactoryDataTransformer
    config : dict

def runner_transformer_data(catalogue_data_transformer:list[DataTransformerObject], data:DataFrame):
    for element in catalogue_data_transformer:
        transformer = element.transformer.get_data_tranformer
        data = transformer.run(data,element.config)
    return data