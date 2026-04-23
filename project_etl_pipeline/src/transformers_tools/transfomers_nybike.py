from interfaces import DataTransformer
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf,coalesce,when,col,lit,year,quarter,dayofmonth,month,dayofweek,date_format,concat,to_timestamp,udf,isnull ,when
from pyspark.sql.column import Column
# from utils import get_distnace
from enum import Enum
from pydantic import BaseModel
from typing import Optional, Callable,Union
import uuid
from transformers_generic import add_or_update_column



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

class AddRideType(DataTransformer):
    def run(self, df: DataFrame, config: Optional[dict]) -> DataFrame:
        print('Ride type add column initiated')
        return add_or_update_column(
            df,
            'enr_rideable_type',
            lambda df: when(col('rideable_type').isNull(), lit('classic_bike')).otherwise(col('rideable_type'))
        )
    
class AddRideTypeId(DataTransformer):
    def run(self, df: DataFrame, config: Optional[dict]) -> DataFrame:
        print('Ride type add column initiated')
        return add_or_update_column(
            df,
            'enr_rideable_type_id',
            lambda df: when(col('enr_rideable_type') =='classic_bike', lit(1)).otherwise(lit(2))
        )




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