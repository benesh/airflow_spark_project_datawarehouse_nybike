from pyspark.sql import SparkSession
from pyspark import SparkFiles
from transformers import DataTransformerObject,FactoryDataTransformer
from helpers_utils import config_reader  
from datetime import datetime
from etl_metadata import Audit_Batch_Etl,log_to_audit_metadata,Data_To_Process, get_row_to_process,log_to_data_to_porcess
from model_data import silver_schema_ny_bike
import traceback
from steps_pipeline import StepsPipelinesEtl
from settings import PATH_FILES


def create_branch(start_time,spark,config,process_name) -> str :
    ## create the name of
    string_date = str(start_time)
    string_date_str = string_date.replace(' ', '_').replace(':','_').replace('.','_').replace('-','_')
    branch_name=f"process_{process_name}__{string_date_str}"
    # Create a new branch from main
    spark.sql(f"CREATE BRANCH IF NOT EXISTS {branch_name} IN {config['target']['catalog_name']} FROM main")
    # Switch to the new branch
    spark.sql(f"USE REFERENCE {branch_name} IN {config['target']['catalog_name']}")

    ## Merge the branch to the main after write succeded 
    # spark.sql(f"MERGE BRANCH {branch_name} INTO {config['main_branch']} IN {config['catalog_name']}")
    # spark.sql(f"DROP BRANCH IF EXISTS {branch_name} IN {config['catalog_name']}")

    return branch_name



def run_etl(spark:SparkSession,data_to_process:Data_To_Process,config:dict,bronze_catalog_transformer :list = None):
    end_time=None
    rows_processed = 0
    start_time = datetime.now()
    status_audit_processing = None
    # data_to_process.status = config['etl_conf']['properties_data_etl']['process_status_success']
    error_message = None
    branch_silver = None

    try:
        # initialize step transformers
        pipeline = StepsPipelinesEtl(spark=spark, catalog_transformer=bronze_catalog_transformer,config=config)
        #create branch and switch to it
        branch_silver = create_branch(start_time,spark,config,config['etl_conf']['properties_data_etl']['process_name'])
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
        data_to_process.branch_silver = branch_silver
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
    .appName("spark-etl_nybike_silver") \
        .getOrCreate()

    path_file=SparkFiles.get("config_etl_silver_v2_iceberg.yaml")
    config = config_reader(path=path_file)

    data_to_process :Data_To_Process = get_row_to_process(
        config['etl_conf']['properties_data_etl']['process_status_faillure'],
        config['etl_conf']['properties_data_etl']['process_status']
        )
        
    if data_to_process is not None:
        config['source']['dw_period_tag'] = data_to_process.period_tag 
        config['source']['column_partition'] = 'dw_period_tag'
        config['source']['value_partition'] = data_to_process.period_tag 
        config['etl_conf']['schema'] = silver_schema_ny_bike

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

        run_etl(spark = spark, data_to_process = data_to_process , config = config, bronze_catalog_transformer = catalog_transformer)
    else:
        print("No data available to precess in Silver Layer")
    
    spark.stop()










