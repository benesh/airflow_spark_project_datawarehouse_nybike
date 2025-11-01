from pyspark.sql import SparkSession
from pyspark import SparkFiles
from helpers_utils import config_reader ,create_and_switch_to_branch
from datetime import datetime
from etl_metadata import Audit_Batch_Etl,log_to_audit_metadata,Data_To_Process, get_row_to_process,log_to_data_to_porcess
import traceback
from steps_pipeline import StepsPipelinesEtl
from typing import Any


def run_etl(spark:SparkSession, data_to_process:Data_To_Process, config:dict, catalog_transformer :list[Any] = None):
    end_time=None
    rows_processed = 0
    start_time = datetime.now()
    status_audit_processing = None
    error_message = None
    branch_gold = None

    try:
         # initialize step transformers
        pipeline = StepsPipelinesEtl(spark=spark, catalog_transformer=catalog_transformer,config=config)
        #create branch and switch to it
        branch_gold = create_and_switch_to_branch(start_time,spark,config)
        # run pipeline
        rows_processed += pipeline.run()
    
        status_audit_processing = config['etl_conf']['properties_audit']['process_status_success']
        data_to_process.status = config['etl_conf']['properties_data_etl']['process_status_success']
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
        data_to_process.branch_gold = branch_gold
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
    .appName("spark-etl_nybike_gold") \
        .getOrCreate()

    path_file=SparkFiles.get("config_etl_gold_v2_iceberg.yaml")
    config = config_reader(path=path_file)

    # Get the row to process
    data_to_process :Data_To_Process = get_row_to_process(
        config['etl_conf']['properties_data_etl']['process_status_faillure'],
        config['etl_conf']['properties_data_etl']['process_status']
        )
    # audit_process_etl = get_by_id_audit_batch_etl(data_to_process.id)
    if data_to_process is not None:
        config['source']['value_partition'] = data_to_process.period_tag
        run_etl(spark = spark, data_to_process = data_to_process , config = config, catalog_transformer = None)
    else:
        print(f"No data available to process in {config['etl_conf']['properties_data_etl']['process_name']} Layer")
    spark.stop()









