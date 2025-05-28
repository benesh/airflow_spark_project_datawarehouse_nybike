from interfaces import DataTransformer
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf,coalesce,when,col,lit,year,quarter,dayofmonth,month,dayofweek,date_format,concat,to_timestamp,udf,isnull ,when
from pyspark.sql.column import Column
# from utils import get_distnace
from enum import Enum
from pydantic import BaseModel
from typing import Optional, Callable,Union
import uuid

ExpressionType = Union[Column, Callable[[DataFrame], Column]]

def add_or_update_column(
    df: DataFrame,
    column_name: str,
    expression: ExpressionType
) -> DataFrame:
    """
    Adds or updates a column in the DataFrame.
    
    Supports both direct Column expressions and callable functions that return a Column.
    """
    if isinstance(expression, Column):
        # If it's already a Column, use it directly
        return df.select('*',expression.alias(column_name))
    else:
        # If it's a function, call it with the DataFrame to get the Column
        return df.select('*', expression(df).alias(column_name))


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
            df = add_or_update_column(df,"enr_gender",
                                 when(col('gender') == 1,lit("Male"))
                                 .when(col('gender') == 2,lit('Female'))
                                 .otherwise(lit('Unknown')))
        else:
            df = add_or_update_column(df,"enr_gender",lit("Unknown"))
        return df
    
class TransformCustomerTypeFormat(DataTransformer):
    def run(self,df:DataFrame, config:Optional[dict]) -> DataFrame:
        print("Customer type transformation initiated")
        return add_or_update_column(df,"enr_user_type",
                             when(col("user_type") == 'Subscriber',lit("member"))
                             .when(col("user_type") == 'Customer',lit("casual"))
                             .otherwise(col("user_type"))
                            )


class AddColumnDiffTime(DataTransformer):
    def run(self,df:DataFrame,config:Optional[dict]) -> DataFrame:
        print("Duration calculation initiated")
        if config['column_ancien'] not in df.columns:
            return add_or_update_column(df,
                                config['column_result'],                                
                                lambda df: col(config['column_greather']).cast('long') - col(config['colmun_lesser']).cast('long')
                                )
        return add_or_update_column(df,config['column_result'],col(config['column_ancien']))
    
    
class FillNa(DataTransformer):
    """
    Transformer that fill the dataframe with the value that passed by
    param:
        df : Datafame 
        config : dict {value_to_fill:n/a} example 
    """
    def run(self,df:DataFrame,config:Optional[dict]) -> DataFrame:
        print("Duration calculation initiated")
        return df.fillna(config['value_for_fill'])


# class AddRideType(DataTransformer):
#     def run(self,df:DataFrame,config:Optional[dict]) -> DataFrame:
#         print('Ride type add column initiate')
#         return df.withColumn('rideable_type',when(col('rideable_type').isNull(), lit('classic_bike')).otherwise(col('rideable_type')))


class AddRideType(DataTransformer):
    def run(self, df: DataFrame, config: Optional[dict]) -> DataFrame:
        print('Ride type add column initiated')
        return add_or_update_column(
            df,
            'enr_rideable_type',
            lambda df: when(col('rideable_type').isNull(), lit('classic_bike')).otherwise(col('rideable_type'))
        )

# class  AddDimensionsForTimes(DataTransformer):
#     def run(self,df:DataFrame,config:Optional[dict]):
#         print("Dimensions time column add initiated")
#         return df.withColumn('year', year(config['datetime_column'])) \
#              .withColumn('month', month(config['datetime_column'])) \
#              .withColumn('quarter', quarter(config['datetime_column'])) \
#              .withColumn('quarter_name', concat(col('year'), lit('Q'), col('quarter'))) \
#              .withColumn('day', dayofmonth(config['datetime_column'])) \
#              .withColumn('weekday', dayofweek(config['datetime_column'])) \
#              .withColumn('month_name', date_format(config['datetime_column'],'MMMM')) \
#              .withColumn('weekday_name', date_format(config['datetime_column'],'EEEE'))


class  AddDimensionsForTimes(DataTransformer):
    def run(self,df:DataFrame,config:Optional[dict]):
        print("Dimensions time column add initiated")
        columns = [
            ('enr_year', year(col(config['datetime_column']))),
            ('enr_month', month(col(config['datetime_column']))),
            ('enr_quarter', quarter(col(config['datetime_column']))),
            ('enr_day', dayofmonth(col(config['datetime_column']))),
            ('enr_weekday', dayofweek(col(config['datetime_column']))),
            ('enr_month_name', date_format(col(config['datetime_column']), 'MMMM')),
            ('enr_weekday_name', date_format(col(config['datetime_column']), 'EEEE')),
            ('enr_quarter_name', concat(col('enr_year'), lit('Q'), col('enr_quarter')))
        ]
        for (col_name,expression) in columns:
            df = add_or_update_column(df,column_name=col_name,expression=expression)
        return df
    
    ## add literal value in the dataframe 
class AddColumnWithLiteralValue(DataTransformer):
    def run(self,df:DataFrame,config:Optional[dict]):
        print("Column With value literal initiated")
        # df = df.withColumn(config['column_to_add']['column_name'],lit(config['column_to_add']['column_value']))
        return add_or_update_column(
            df,
            config['column_to_add']['column_name'],
            lambda df: lit(config['column_to_add']['column_value'])
        )
    
   
    ## Transformer cast to the datamodel
class CastToDatamodel(DataTransformer):
    def run(self,df:DataFrame,config:Optional[dict]):
        print("Cast To Datamodel initiated")
        ## list de 

        data_model_from_df = list(dict(df.dtypes).keys())

        ## 
        filtered_column = list(filter( lambda x: x not in data_model_from_df ,config['schema'].names ))
        if len(filtered_column) > 0 :
            for column in filtered_column:
                df =  add_or_update_column(df,column, lit(None))

        df = df.select([
            col(field.name).cast(field.dataType).alias(field.name) 
            for field in config['schema'].fields
        ])
        return df
    
@udf
def uuid_gen():
    return str(uuid.uuid4())
    
    ## Add a column to like a uuid
class AddUuidToColumnID(DataTransformer):
    def run(self,df:DataFrame,config:Optional[dict]):
        print(f"Add uuid in to th column: {config['column_id']}")
        return add_or_update_column(
            df,
            config['column_id'],
            uuid_gen()
        )


@udf
def id_gen():
    return str(uuid.uuid4())
    
    ## Add a column to like a uuid
class AddIdColumnID(DataTransformer):
    def run(self,df:DataFrame,config:Optional[dict]):
        print(f"Add uuid in to th column: {config['column_id']}")
        return add_or_update_column(
            df,
            config['column_id'],
            concat(month(col('start_at')),)
        )
    

    ## Cast column to timstamp
class CastToTimestamp(DataTransformer):
    """
    Cast date format 
    
    """
    def run(self,df:DataFrame,config:Optional[dict]):
        print(f"Cast to timestamp: {config['cast_to_timestamp']}")
        regex_pattern = r"^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}$"  ## determine if the date is at format 00/00/000 00:00 
        for column in config['cast_to_timestamp'] :
            df = df.withColumn(column,
                               when(col(column).rlike(regex_pattern),concat(col(column),lit(':00'))).otherwise(col(column)))
            df = df.withColumn(column, coalesce(
                            to_timestamp(col(column), 'M/d/yyyy H:mm:ss'),
                            # to_timestamp(col(column), "yyyy-MM-dd HH:mm:ss"),
                            to_timestamp(col(column))
                                        )
                        )
        return df

class Cast_To(DataTransformer):
    def run(self,df:DataFrame,config:Optional[dict]):
        print(f"Reconcile schema df.to: {config['schema']}")
        return df.to(config['schema'])
    
    
class Cast_To(DataTransformer):
    def run(self,df:DataFrame,config:Optional[dict]):
        print(f"Reconcile schema df.to: {config['schema']}")
        return df.to(config['schema'])

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
    ADD_UUID_TO_COLUMN_ID='add_uuid_to_column_id'
    CAST_TO_TIMESTAMP='cast_to_timestamp'
    FILL_NA='fill_na'
    CAST_TO='cast_to'
    
    @property
    def get_data_tranformer(self)->DataTransformer:
        return {
            self.RENAME_COLUMNS: RenameColumn(),
            self.DROP_COLUMNS: DropColumns(),
            self.GENDER_TRANSFORMER_OR_ADD: TransformCustomerGenderFormat(),
            self.ADD_COLUMN_DIFF_TIME: AddColumnDiffTime(),
            self.ADD_BIKE_TYPE : AddRideType(),
            self.TRANSFORM_CUSTOMER_COLUMN: TransformCustomerTypeFormat(),
            self.ADD_DIMENSIONS_TIME: AddDimensionsForTimes(),
            self.ADD_COLUMN_WITH_LITERAL_VALUE: AddColumnWithLiteralValue(),
            self.CAST_TO_DATAMODEL: CastToDatamodel(),
            self.ADD_UUID_TO_COLUMN_ID: AddUuidToColumnID(),
            self.CAST_TO_TIMESTAMP: CastToTimestamp(),
            self.FILL_NA: FillNa(),
            self.CAST_TO: Cast_To()
        }[self]

class DataTransformerObject(BaseModel):
    transformer : FactoryDataTransformer
    config : dict

def runner_transformer_data(catalogue_data_transformer:list[DataTransformerObject], data:DataFrame):
    for element in catalogue_data_transformer:
        transformer = element.transformer.get_data_tranformer
        data = transformer.run(data,element.config)
    return data