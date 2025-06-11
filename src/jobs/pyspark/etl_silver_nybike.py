import traceback
from pyspark.sql import SparkSession
from pyspark import SparkFiles
# from concurrent.futures import ThreadPoolExecutor
from transformers import runner_transformer_data,DataTransformerObject,FactoryDataTransformer
from readers import FactoryReader
from sinkersType import FactorySinkData
from helpers_utils import config_reader,get_row_to_process
from datetime import datetime
from etl_metadata import ETL_Metadata,log_etl_metadata,get_data_to_process,Data_To_Process,update_data_to_porcess
from model_data import silver_schema_ny_bike


def run(spark:SparkSession,data_to_process:Data_To_Process,config:dict):
    try:
        # Step 1: Capture start time and initialize metadata
        start_time = datetime.now()
        metadata = ETL_Metadata(
            process_name="ETL_TO_SILVER_LAYER",
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
                transformer= FactoryDataTransformer.CAST_TO_TIMESTAMP,
                config=config['etl_conf']
            ),
            DataTransformerObject(
                transformer= FactoryDataTransformer.ADD_COLUMN_DIFF_TIME,
                config=config['etl_conf']['diff_column']
            ),
            DataTransformerObject(
                transformer= FactoryDataTransformer.GENDER_TRANSFORMER_OR_ADD,
                config={}
            ),
            DataTransformerObject(
                transformer= FactoryDataTransformer.TRANSFORM_CUSTOMER_COLUMN,
                config= {}
            ),
            DataTransformerObject(
                transformer=FactoryDataTransformer.ADD_DIMENSIONS_TIME,
                config=config['etl_conf']['dimensions_time']
            ),
            DataTransformerObject(
                transformer=FactoryDataTransformer.ADD_BIKE_TYPE,
                config={}
            ),
            DataTransformerObject(
                transformer=FactoryDataTransformer.ADD_BIKE_TYPE_ID,
                config={}
            ),
            DataTransformerObject(
                transformer=FactoryDataTransformer.ADD_UUID_TO_COLUMN_ID,
                config=config['etl_conf']
            ),
            DataTransformerObject(
                transformer= FactoryDataTransformer.CAST_TO_DATAMODEL,
                config=config['etl_conf']
            
            ),
            DataTransformerObject(
                transformer= FactoryDataTransformer.CAST_TO,
                config=config['etl_conf']
            )
        ]

        df = FactoryReader().getDataframe(spark,config['source'])
        df = runner_transformer_data(catalog_transformer,df)

        ## create the name of 
        string_date = str(start_time)
        string_date_str = string_date.replace(' ', '_').replace(':','_').replace('.','_').replace('-','_')
        branch_name=f"feature_{metadata.process_name}__{string_date_str}"
        # Create a new branch from main
        spark.sql(f"CREATE BRANCH IF NOT EXISTS {branch_name} IN warehouse FROM main")
        # Switch to the new branch
        spark.sql(f"USE REFERENCE {branch_name} IN warehouse")
        

        FactorySinkData().run(df,config['target'])

        ## Merge the branch to the main after write succeded
        spark.sql(f"MERGE BRANCH {branch_name} INTO main IN warehouse")
        
        # Step 3: Capture end time and update metadata
        end_time = datetime.now()
        rows_processed = df.count()
        duration = end_time - start_time
        metadata.end_time = end_time
        metadata.duration = duration
        metadata.rows_processed = rows_processed
        metadata.status = "SUCCESS"
        log_etl_metadata(metadata)  # Update metadata

        data_to_process.status='TO_GOLD_LAYER'
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

        data_to_process.status='FAILURE_TO_SILVER_LAYER'
        update_data_to_porcess(data_to_process)
        traceback.print_exc()
        print(f"ETL process failed: {e}")

if __name__ == "__main__":
    
    spark = SparkSession.builder \
    .appName("spark-etl_nybike_silver") \
        .getOrCreate()

    path_file=SparkFiles.get("config_etl_silver_v2_iceberg.yaml")
    config = config_reader(path=path_file)

    data_to_process_list = get_row_to_process('FAILURE_TO_SILVER_LAYER','TO_SILVER_LAYER')
    if len(data_to_process_list) > 0 :
        data_to_process = data_to_process_list[0]
        config['source']['dw_period_tag'] = data_to_process.period_tag 
        config['etl_conf']['schema'] = silver_schema_ny_bike 
        run(spark,data_to_process = data_to_process,config = config)
    else:
        print("No data available to precess in Silver Layer")
    
        spark.stop()










