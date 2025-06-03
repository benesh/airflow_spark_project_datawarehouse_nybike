import os
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkFiles
from transformers import runner_transformer_data,DataTransformerObject,FactoryDataTransformer
from readers import FactoryReader
from sinkersType import FactorySinkData
from helpers_utils import config_reader,list_files_with_format,get_row_to_process
from datetime import datetime
from etl_metadata import ETL_Metadata,log_etl_metadata,Data_To_Process,update_data_to_porcess
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
            )
            # ,
            # DataTransformerObject(
            #     transformer= FactoryDataTransformer.CAST_TO_DATAMODEL,
            #     config = config['etl_conf']
            # )
        ]
        groupe_files = config['source']['group_file_to_read']
        rows_processed = 0
        list_files = list_files_with_format(directory=f"{config['source']['root_path']}/{config['source']['path_csv']}", format_file=".csv")
        for i in range(0,len(list_files),groupe_files):
            sub_list_files = list_files[i:i + groupe_files]
            config['source']['list_csv']=sub_list_files
            # reead the files
            df = FactoryReader().getDataframe(spark,config['source'])
            #transforms files
            df = runner_transformer_data(catalog_transformer,df)
            # Insert column that is not presen in the Dataframe with None value 
            df = df.to(bronze_schema_ny_bike)
            # df.show()
            # df.printSchema()
            # write files
            FactorySinkData().run(df,config['target'])
            rows_processed += df.count()


        # # reead the files
        # df = FactoryReader().getDataframe(spark,config['source'])
        # #transforms files
       
        # df = runner_transformer_data(catalog_transformer,df)
        # # write files
        # df = df.to(bronze_schema_ny_bike)
        
        # FactorySinkData().run(df,config['target'])
        
        # Step 3: Capture end time and update metadata
        end_time = datetime.now()
        # rows_processed = df.count()
        duration = end_time - start_time
        metadata.end_time = end_time
        metadata.duration = duration
        metadata.rows_processed = rows_processed
        metadata.status = "SUCCESS"
        log_etl_metadata(metadata)  # Update metadata

        data_to_process.status='TO_SILVER_LAYER'
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
    
    path_file=SparkFiles.get("config_etl_bronze_v2_iceberg.yaml")
    config = config_reader(path=path_file)

    data_to_process_list = get_row_to_process('FAILURE_TO_BRONZE_LAYER','TO_BRONZE_LAYER')
    print(data_to_process_list[0])
    print(len(data_to_process_list)>0)
    if len(data_to_process_list) > 0 :
        data_to_process = data_to_process_list[0]
        print("Enter to etl _bonze")
        config['source']['path_csv'] = data_to_process.path_csv
        config['etl_conf']['column_to_add']['column_value'] =data_to_process.period_tag
        config['etl_conf']['schema'] = bronze_schema_ny_bike
        config['target']['dw_period_tag'] = bronze_schema_ny_bike

        run(spark = spark, data_to_process = data_to_process , config = config)
    else:
        print("No data available to precess in Bronze Layer")
