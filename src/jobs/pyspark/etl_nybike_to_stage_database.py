from pyspark.sql import SparkSession
from concurrent.futures import ThreadPoolExecutor
from transformers import runner_transformer_data,DataTransformerObject,FactoryDataTransformer
from readers import FactoryReader
from sinkersType import FactorySinkData
from model_data import ModelDatawahouseNYBike,ModelDatawahouseNYBikeV2
from helpers_utils import config_reader
import time
import yaml
import os
from datetime import datetime
from etl_metadata import ETL_Metadata,log_etl_metadata,get_data_to_process,Data_To_Process,update_data_to_porcess



def run(data_to_process:Data_To_Process,config:dict):
    try:
        # Step 1: Capture start time and initialize metadata
        start_time = datetime.now()
        metadata = ETL_Metadata(
            process_name="ETL_TO_STAGE_DATABASE",
            start_time=start_time,
            status="IN_PROGRESS",
            process_period=data_to_process.process_period,
            year=data_to_process.year,
            month=data_to_process.month,
            data_to_process_id_fk=data_to_process.id
        )
        log_etl_metadata(metadata)  # Log initial metadata
        
        catalog_transformer = [
            DataTransformerObject(
                transformer= FactoryDataTransformer.RENAME_COLUMNS,
                config = config['etl_conf']
            ),
            DataTransformerObject(
                transformer= FactoryDataTransformer.DROP_COLUMNS,
                config = config["etl_conf"]
            ),
            DataTransformerObject(
                transformer= FactoryDataTransformer.ADD_BIKE_TYPE,
                config = {}
            ),
            DataTransformerObject(
                transformer= FactoryDataTransformer.ADD_TRIP_DURATION,
                config = {}
            ),
            DataTransformerObject(
                transformer= FactoryDataTransformer.GENDER_TRANSFORMER_OR_ADD,
                config = {}
            ),
            DataTransformerObject(
                transformer= FactoryDataTransformer.ADD_COLUMN_IDS,
                config = config['etl_conf']
            ),
            DataTransformerObject(
                transformer= FactoryDataTransformer.TRANSFORM_CUSTOMER_COLUMN,
                config = {}
            ),
            DataTransformerObject(
                transformer=FactoryDataTransformer.ADD_DIMENSIONS_TIME,
                config=config['etl_conf']['dimensions_time']
            )
        ]

        spark = SparkSession.builder \
        .appName("spark-submit-ubuntu") \
            .getOrCreate()
        
        df = FactoryReader().getDataframe(spark,config['source'])
        df = runner_transformer_data(catalog_transformer,df).cache()

        model = ModelDatawahouseNYBikeV2()
        
        dim_date_df,dim_date_config = model.get_df_DIMDATE(df,config['target'])
        FactorySinkData().run(dim_date_df,dim_date_config)

        dim_station_df,dim_station_config = model.get_df_DIM_STATION_NYBIKE(df,config['target'])
        FactorySinkData().run(dim_station_df,dim_station_config)

        fact_df,fact_config = model.get_df_DIM_FACT (df,config['target'])
        FactorySinkData().run(fact_df,fact_config)
        
        # Step 3: Capture end time and update metadata
        end_time = datetime.now()
        rows_processed = df.count()
        duration = end_time - start_time
        metadata.end_time = end_time
        metadata.duration = duration
        metadata.rows_processed = rows_processed
        metadata.status = "SUCCESS"
        log_etl_metadata(metadata)  # Update metadata

        data_to_process.status='TO_PROD_DATABASE'
        update_data_to_porcess(data_to_process)
        

        print("ETL process completed successfully.")

    except Exception as e:
        # Step 4: Handle errors and update metadata
        end_time = datetime.now()
        duration = end_time - start_time
        metadata.end_time = end_time
        metadata.duration = duration
        metadata.status = "FAILURE"
        metadata.error_message = str(e)
        log_etl_metadata(metadata)  # Update metadata with error details

        data_to_process.status='FAILURE_TO_STAGE'
        update_data_to_porcess(data_to_process)

        print(f"ETL process failed: {e}")

if __name__ == "__main__":
    path_file='./jobs/handler_services/configs_job/config_etl_1.yaml'
    config = config_reader(path=path_file)
    result = get_data_to_process("TO_STAGE_DATABASE")
    # for data_to_process in result:
    #     config['source']['path_csv'] = data_to_process.path_csv
    #     config['source']['month'] = data_to_process.month
    #     config['source']['year'] = data_to_process.year
    #     config['source']['reader'] = data_to_process.reader 
    #     config['source']['process_period'] = data_to_process.process_period 
       
    #     run(data_to_process = data_to_process,config = config)
    
    data_to_process = result[0]
    config['source']['path_csv'] = data_to_process.path_csv
    config['source']['month'] = data_to_process.month
    config['source']['year'] = data_to_process.year
    config['source']['reader'] = data_to_process.reader 
    config['source']['process_period'] = data_to_process.process_period 
    run(data_to_process = data_to_process,config = config)








