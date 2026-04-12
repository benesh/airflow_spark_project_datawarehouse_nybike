import sys
import os

sys.path.append(os.path.abspath("src/jobs/pyspark"))

from chispa.dataframe_comparer import *
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql.types import StructType, StructField, StringType, IntegerType , TimestampType , DoubleType , FloatType, DateType
from pyspark.sql.functions import udf,coalesce,when,col,lit,year,quarter,dayofmonth,month,dayofweek,date_format,concat,to_timestamp,udf,isnull ,when

from interfaces import DataTransformer
from transformers import RenameColumn,AddColumnDiffTime,DropColumns,CastToTimestamp
from model_data import bronze_schema_ny_bike
from helpers_utils import config_reader


from collections import namedtuple

sample_old = [{"tripduration":"695",
               "starttime":"2013-06-01 00:00:01",
               "stoptime":"2013-06-01 00:11:36",
               "start station id":"444",
               "start station name":"Broadway & W 24 St",
               "start station latitude":"40.7423543",
               "start station longitude":"-73.98915076",
               "end station id":"434.0",
               "end station name":"9 Ave & W 18 St",
               "end station latitude":"40.74317449",
               "end station longitude":"-74.00366443",
               "bikeid":"19678",
               "usertype":"Subscriber",
               "birth year":"1983.0",
               "gender":"1"}]

sample_new = [{"trip_duration":"695",
               "start_at":"2013-06-01 00:00:01",
               "stop_at":"2013-06-01 00:11:36",
               "start_station_id":"444",
               "start_station_name":"Broadway & W 24 St",
               "start_station_latitude":"40.7423543",
               "start_station_longitude":"-73.98915076",
               "end_station_id":"434.0",
               "end_station_name":"9 Ave & W 18 St",
               "end_station_latitude":"40.74317449",
               "end_station_longitude":"-74.00366443",
               "bike_id":"19678",
               "user_type":"Subscriber",
               "customer_year_birth":"1983.0",
               "gender":"1"}]


def test_Renamed_Dataframe(spark_session):

    config = config_reader('/home/ben/workspace/airflow_spark_project_datawarehouse_nybike/src/resources/configs/config_etl_bronze_v2_iceberg.yaml')

    source_df = spark_session.createDataFrame(sample_old)
    rename_obj= RenameColumn()
    result_df = rename_obj.run(source_df,config['etl_conf'])
    result_df = result_df.to(bronze_schema_ny_bike)
    
    expected_df = spark_session.createDataFrame(sample_new)
    expected_df = expected_df.to(bronze_schema_ny_bike)

    assertDataFrameEqual(result_df,expected_df)

def test_Drop_column_transformers(spark_session):
    input_data = [{"trip_duration":"695",
               "start_at":"2013-06-01 00:00:01",
               "stop_at":"2013-06-01 00:11:36",
               "start_station_id":"444"
               }]
    expected_data = [{
               "start_at":"2013-06-01 00:00:01",
               "stop_at":"2013-06-01 00:11:36"
              
               }]
    config={
        "columns_to_delete":["trip_duration","start_station_id"]
    }
    df_input_data = spark_session.createDataFrame(input_data)
    df_expected_data = spark_session.createDataFrame(expected_data)

    drop_transformer = DropColumns()
    df_result_transformed = drop_transformer.run(df_input_data,config)

    assertDataFrameEqual(df_result_transformed,df_expected_data)

def test_transformer_cast_time_type_1(spark_session):
    input_data = [{
               "start_at":"2013-06-01 00:00:01",
               "stop_at":"2013-06-01 00:11:36"
               }]
    
    expected_data = [{
               "start_at":"2013-06-01 00:00:01",
               "stop_at":"2013-06-01 00:11:36"             
               }]
    
    config={
        "cast_to_timestamp":["start_at","stop_at"]
    }
    transformer_cast = CastToTimestamp()
    df_input_data=spark_session.createDataFrame(input_data)
    df_expected_data=spark_session.createDataFrame(expected_data)
    df_expected_data = df_expected_data\
        .withColumn('start_at',to_timestamp(col('start_at')))\
        .withColumn('stop_at',to_timestamp(col('stop_at')))
    result_transformation = transformer_cast.run(df_input_data,config)

    assertDataFrameEqual(result_transformation,df_expected_data)



def test_transformer_cast_time_type_2(spark_session):
    input_data = [{
               "start_at":"2/1/2016 00:00:08",
               "stop_at":"2/1/2016 00:07:08"
               }]
    
    expected_data = [{
               "start_at":"2016-02-01 00:00:08",
               "stop_at":"2016-02-01 00:07:08"             
               }]
    
    # expected_schema= StructType([
    #         StructField("start_at", TimestampType(), nullable=True),
    #         StructField("stop_at", TimestampType(), nullable=True),
    # ])
    
    config={
        "cast_to_timestamp":["start_at","stop_at"]
    }
    transformer_cast = CastToTimestamp()
    df_input_data=spark_session.createDataFrame(input_data)
    df_expected_data=spark_session.createDataFrame(expected_data)
    df_expected_data = df_expected_data\
        .withColumn('start_at',to_timestamp(col('start_at')))\
        .withColumn('stop_at',to_timestamp(col('stop_at')))
    result_transformation = transformer_cast.run(df_input_data,config)

    assertDataFrameEqual(result_transformation,df_expected_data)

def test_transformer_cast_time_type_3(spark_session):
    input_data = [{
               "start_at":"10/1/2015 00:00:02",
               "stop_at":"10/1/2015 00:02:54"
               }]
    
    expected_data = [{
               "start_at":"2015-10-01 00:00:02",
               "stop_at":"2015-10-01 00:02:54"             
               }]
    
    config={
        "cast_to_timestamp":["start_at","stop_at"]
    }
    transformer_cast = CastToTimestamp()
    df_input_data=spark_session.createDataFrame(input_data)
    df_expected_data=spark_session.createDataFrame(expected_data)
    df_expected_data = df_expected_data\
        .withColumn('start_at',to_timestamp(col('start_at')))\
        .withColumn('stop_at',to_timestamp(col('stop_at')))
    result_transformation = transformer_cast.run(df_input_data,config)

    assertDataFrameEqual(result_transformation,df_expected_data)



def test_transformer_cast_time_type_4(spark_session):
    input_data = [{
               "start_at":"1/1/2015 0:01",
               "stop_at":"1/1/2015 0:24"
               }]
    
    expected_data = [{
               "start_at":"2015-1-01 00:01:00",
               "stop_at":"2015-1-01 00:24:00"             
               }]
    config={
        "cast_to_timestamp":["start_at","stop_at"]
    }
    transformer_cast = CastToTimestamp()
    df_input_data=spark_session.createDataFrame(input_data)
    df_expected_data=spark_session.createDataFrame(expected_data)
    df_expected_data = df_expected_data\
        .withColumn('start_at',to_timestamp(col('start_at')))\
        .withColumn('stop_at',to_timestamp(col('stop_at')))
    result_transformation = transformer_cast.run(df_input_data,config)

    assertDataFrameEqual(result_transformation,df_expected_data)




