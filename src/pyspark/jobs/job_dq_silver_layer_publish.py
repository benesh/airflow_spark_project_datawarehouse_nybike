from pyspark.sql import SparkSession
from pyspark import SparkFiles
from helpers_utils import config_reader  
from datetime import datetime
from etl_metadata import Audit_Batch_Etl,log_to_audit_metadata,Data_To_Process, get_row_to_process,log_to_data_to_porcess,get_by_id_audit_batch_etl
import traceback
from steps_pipeline import StepsPipelinesDataQuality
from data_quality import DataQualityTester

def run_dq(spark:SparkSession,data_to_process:Data_To_Process,dq_catalog_list:list,config:dict):
    start_time = datetime.now()
    end_time=None
    rows_processed = 0
    status_audit_processing = config['dq_conf']['properties_audit']['process_status_success']
    data_to_process.status = config['dq_conf']['properties_data_dq']['process_status_success']
    error_message = None
    result_details = None

    try:
        pipeline = StepsPipelinesDataQuality(spark=spark, catalog_data_quality=dq_catalog_list,config=config)

        #run pipeline
        result_details = pipeline.run()

        # Step 3: Capture end time and update metadata
        end_time = datetime.now()

    except Exception as e:
        # Step 4: Handle errors and update metadata
        end_time = datetime.now()
        status_audit_processing = config['dq_conf']['properties_audit']['process_status_faillure']
        data_to_process.status = config['dq_conf']['properties_data_dq']['process_status_faillure']
        error_message = str(e)
        traceback.print_exc()
        print(f"Data Quality process failed: {e}")

    finally:
        # data_to_process.branch_silver = branch_silver
        metadata = Audit_Batch_Etl(
            process_name=config['dq_conf']['properties_data_dq']['process_name'],
            start_time=start_time,
            end_time=end_time,
            duration=end_time - start_time,
            rows_processed =rows_processed,
            status=status_audit_processing,
            process_period=data_to_process.period_tag,
            year=data_to_process.year,
            month=data_to_process.month,
            data_to_process_id_fk=data_to_process.id,
            error_message=error_message,
            result_details = result_details
        )
        log_to_audit_metadata(metadata)  # Add metadata with error details
        log_to_data_to_porcess(data_to_process)  # Update metadata with error details

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Spark_job_dq_nybike_silver") \
            .getOrCreate()
    
    path_file = SparkFiles.get("config_job_dq_silver.yaml")
    config = config_reader(path=path_file)
    # Get the row to process
    data_to_process :Data_To_Process = get_row_to_process(
        config['dq_conf']['properties_data_dq']['process_status_faillure'],
        config['dq_conf']['properties_data_dq']['process_status']
        )
    audit_process_etl = get_by_id_audit_batch_etl(data_to_process.id)
    if data_to_process is not None:
        config['source']['value_partition'] = data_to_process.period_tag
        config['target']['branch_source'] = data_to_process.branch_silver
        config['dq_conf']['rows_count_check']['expected_rows_count'] = audit_process_etl.rows_processed


        dq_catalog = [
            DataQualityTester(
                test_name= 'rows_count_check',
                config= config['dq_conf']['rows_count_check']
            ),
            DataQualityTester(
                test_name= 'null_check',
                config = config['dq_conf']
            )
        ]

        run_dq(spark = spark, data_to_process = data_to_process , dq_catalog_list = dq_catalog, config = config)
    else:
        print(f"No data available to process in {config['dq_conf']['properties_data_dq']['process_name']} Layer")
    spark.stop()
    