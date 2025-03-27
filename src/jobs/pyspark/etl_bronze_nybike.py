from pyspark.sql import SparkSession
from pyspark import SparkFiles
from concurrent.futures import ThreadPoolExecutor
from transformers import runner_transformer_data,DataTransformerObject,FactoryDataTransformer
from readers import FactoryReader
from sinkersType import FactorySinkData
from helpers_utils import config_reader,list_files_with_format
from datetime import datetime
from etl_metadata import ETL_Metadata,log_etl_metadata,get_data_to_process,Data_To_Process,update_data_to_porcess
from model_data import bronze_schema_ny_bike
import traceback


def run(spark:SparkSession,data_to_process:Data_To_Process,config:dict):
    try:
        # Step 1: Capture start time and initialize metadata
        start_time = datetime.now()
        metadata = ETL_Metadata(
            process_name="ETL_TO_BRONZE_LAYER",
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
                config= config['etl_conf']
            ),
            DataTransformerObject(
                transformer= FactoryDataTransformer.ADD_COLUMN_WITH_LITERAL_VALUE,
                config = config['etl_conf']
            ),
            DataTransformerObject(
                transformer= FactoryDataTransformer.CAST_TO_DATAMODEL,
                config = config['etl_conf']
            )
        ]
        groupe_files = 10
        list_files = list_files_with_format(directory=f"{config['source']['root_path']}/{config['source']['path_csv']}", format_file=".csv")
        for i in range(0,len(list_files),groupe_files):
            sub_list_files = list_files[i:i + groupe_files]
            config['source']['list_csv']=sub_list_files
            # reead the files
            df = FactoryReader().getDataframe(spark,config['source'])
            #transforms files
            df = runner_transformer_data(catalog_transformer,df)
            # write files
            df = df.to(bronze_schema_ny_bike)
            FactorySinkData().run(df,config['target'])
        
        # Step 3: Capture end time and update metadata
        end_time = datetime.now()
        rows_processed = df.count()
        duration = end_time - start_time
        metadata.end_time = end_time
        metadata.duration = duration
        metadata.rows_processed = rows_processed
        metadata.status = "SUCCESS"
        log_etl_metadata(metadata)  # Update metadata

        data_to_process.status='TO_SYLVER_LAYER'
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

        data_to_process.status='FAILURE_TO_BRONZE_LAYER'
        update_data_to_porcess(data_to_process)
        traceback.print_exc()
        print(f"ETL process failed: {e}")

if __name__ == "__main__":

    spark = SparkSession.builder \
        .appName("spark-etl_nybike_bronze") \
            .getOrCreate()
    # path_file='/opt/airflow/resources/configs/config_etl_1_v2.yaml'
    path_file=SparkFiles.get("config_etl_1_v2_iceberg.yaml")
    config = config_reader(path=path_file)
    result = get_data_to_process("FAILURE_TO_BRONZE_LAYER")
    # result = get_data_to_process("TO_BRONZE_LAYER")

    # for data_to_process in result:
    #     config['source']['path_csv'] = data_to_process.path_csv
    #     config['source']['month'] = data_to_process.month
    #     config['source']['year'] = data_to_process.year
    #     config['source']['reader'] = data_to_process.reader 
    #     config['source']['process_period'] = data_to_process.process_period 
    #     run(data_to_process = data_to_process,config = config)
    
    data_to_process = result[0]
    config['source']['path_csv'] = data_to_process.path_csv

    # config['source']['month'] = data_to_process.month
    # config['source']['year'] = data_to_process.year
    # config['source']['reader'] = data_to_process.reader 
    # config['source']['process_period'] = data_to_process.process_period

    config['etl_conf']['column_to_add']['column_value'] =data_to_process.period_tag
    config['etl_conf']['schema'] = bronze_schema_ny_bike

    run(spark,data_to_process = data_to_process , config = config)
