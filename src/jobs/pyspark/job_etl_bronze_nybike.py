import os
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkFiles
from transformers import DataTransformerObject,FactoryDataTransformer
from helpers_utils import config_reader  
from datetime import datetime
from etl_metadata import Audit_Batch_Etl,log_to_audit_metadata,Data_To_Process, get_row_to_process,log_to_data_to_porcess
from model_data import bronze_schema_ny_bike
import traceback
from steps_pipeline import StepsPipelinesEtl
from settings import PATH_FILES


def run_etl(spark:SparkSession,data_to_process:Data_To_Process,config:dict,bronze_catalog_transformer :list = None):
    start_time = datetime.now()
    end_time=None
    rows_processed = 0
    status_audit_processing = config['etl_conf']['properties_audit']['process_status_success']
    data_to_process.status = config['etl_conf']['properties_data_etl']['process_status_success']
    error_message = None
    try:

        # initialize step transformers
        pipeline = StepsPipelinesEtl(spark=spark, catalog_transformer=bronze_catalog_transformer,config=config)
        # run pipeline
        rows_processed += pipeline.run()

        end_time = datetime.now()

    except Exception as e:
        # Step 4: Handle errors and update metadata
        end_time = datetime.now()
        status_audit_processing = config['etl_conf']['properties_audit']['process_status_faillure']
        data_to_process.status = config['etl_conf']['properties_data_etl']['process_status_faillure']
        error_message = str(e)
        traceback.print_exc()
        print(f"ETL process failed: {e}")

    finally:
        metadata = Audit_Batch_Etl(
            process_name=config['etl_conf']['properties_data_etl']['process_name'],
            start_time=start_time,
            end_time=end_time,
            duration=end_time - start_time,
            rows_processed=rows_processed,
            status=status_audit_processing,
            process_period=data_to_process.period_tag,
            year=data_to_process.year,
            month=data_to_process.month,
            data_to_process_id_fk=data_to_process.id,
            error_message=error_message
        )
        log_to_audit_metadata(metadata)  # Add metadata with error details
        log_to_data_to_porcess(data_to_process)  # Update metadata with error details


if __name__ == "__main__":
    
    spark = SparkSession.builder \
        .appName("Spark-etl_nybike_bronze") \
            .getOrCreate()
    
    path_file = SparkFiles.get("config_etl_bronze_from_s3_to_iceberg.yaml")
    config = config_reader(path=path_file)
    # Get the row to process
    data_to_process :Data_To_Process = get_row_to_process(
        config['etl_conf']['properties_data_etl']['process_status_faillure'],
        config['etl_conf']['properties_data_etl']['process_status']
        )

    if data_to_process is not None:
        config['source'][PATH_FILES] = data_to_process.files
        config['etl_conf']['column_to_add']['column_value'] = data_to_process.period_tag
        config['target']['column_param_overwrite_value'] = data_to_process.period_tag
        config['etl_conf']['schema'] = bronze_schema_ny_bike

        catalog = [
            DataTransformerObject(
                transformer= FactoryDataTransformer.RENAME_COLUMNS,
                config= config['etl_conf']
            ),
            DataTransformerObject(
                transformer= FactoryDataTransformer.ADD_COLUMN_WITH_LITERAL_VALUE,
                config = config['etl_conf']
            ), 
            DataTransformerObject(
                transformer= FactoryDataTransformer.CAST_TO,
                config = config['etl_conf']
            )
        ]

        run_etl(spark = spark, data_to_process = data_to_process , config = config, bronze_catalog_transformer = catalog)
    else:
        print("No data available to process in Bronze Layer")
    spark.stop()
