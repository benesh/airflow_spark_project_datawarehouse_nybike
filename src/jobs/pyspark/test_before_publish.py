from readers import FactoryReader
from sinkersType import FactorySinkData
from pyspark.sql import SparkSession
from tests import test_number_of_rows_v2
from helpers_utils import config_reader
from datetime import datetime
from etl_metadata import ETL_Metadata,log_etl_metadata,Data_To_Process,get_data_to_process,update_data_to_porcess,get_by_id_etl_meatada

def run(spark:SparkSession,data:Data_To_Process,config:dict):
    try:
        # Step 1: Capture start time and initialize metadata
        start_time = datetime.now()
        metadata = ETL_Metadata(
            process_name="ETL_TO_PROD_DATABASE",
            start_time=start_time,
            status="IN_PROGRESS",
            process_period=data.process_period,
            year=data.year,
            month=data.month,
            data_to_process_id_fk=data.id
        )
        log_etl_metadata(metadata)  # Log initial metadata

        # # Create spark session
        # spark = SparkSession.builder \
        # .appName("spark-submit-ubuntu") \
        # .getOrCreate()

        # Create reader factory
        reader = FactoryReader()
        # print(config['source'])
        # Get dataframe of differents dim
        config['source']['dbtable']='dim_date_nybike'
        df_date = reader.getDataframe(spark,config['source'])
        df_date.createOrReplaceTempView('dim_date')

        config['source']['dbtable']='dim_station_nybike'
        df_station = reader.getDataframe(spark,config['source'])
        df_station.createOrReplaceTempView('dim_station')

        config['source']['dbtable']='fact_nybike'
        df_fact = reader.getDataframe(spark,config['source'])
        df_fact.createOrReplaceTempView('dim_fact')
        
        ## Get etl_metadata 
        etl_metada_stage=get_by_id_etl_meatada(data.id)

        # Build WHERE clause conditionally
        month_condition = f"AND month={data.month}" if data.month > 0 else ""

        # test the number of rows
        df_date_result = spark.sql(f""" SELECT * FROM dim_date WHERE YEAR={data.year} {month_condition}""")
        test_number_of_rows_v2(df_date_result,etl_metada_stage.rows_processed)
        df_station_result = spark.sql(f"""SELECT * FROM dim_station WHERE YEAR={data.year} {month_condition}""")
        test_number_of_rows_v2(df_date_result,etl_metada_stage.rows_processed)
        df_fact_result = spark.sql(f"""SELECT * FROM dim_fact WHERE YEAR={data.year} {month_condition}""")
        test_number_of_rows_v2(df_date_result,etl_metada_stage.rows_processed)

        # config['dbtable'] = 'fact_nybike'
        config['target']['dbtable'] = 'dim_date_nybike'
        FactorySinkData().run(df_date_result,config['target'])
        config['target']['dbtable'] = 'dim_station_nybike'
        FactorySinkData().run(df_station_result,config['target'])
        config['target']['dbtable'] = 'fact_nybike'
        FactorySinkData().run(df_fact_result,config['target'])

        # Step 3: Capture end time and update metadata
        end_time = datetime.now()
        duration = end_time - start_time
        metadata.end_time = end_time
        metadata.duration = duration
        metadata.rows_processed = etl_metada_stage.rows_processed
        metadata.status = "SUCCESS"
        log_etl_metadata(metadata)  # Update metadata
        data.status='SUCCES_PRODCUTION'
        update_data_to_porcess(data=data)

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
        data.status='FAILURE_PROD'
        update_data_to_porcess(data=data)

        print(f"ETL process failed: {e}")

if __name__ == "__main__":
    # Create spark session
    spark = SparkSession.builder \
    .appName("spark-submit-ubuntu") \
    .getOrCreate()
    
    path_config='../configs_job/config_etl_2.yaml'
    config = config_reader(path=path_config)
    # result = get_data_to_process("TO_PROD_DATABASE")
    # result = get_data_to_process("FAILURE_PROD")

    # for data_to_process in result:
    #     run(data=data_to_process,config=config)

    data = get_data_to_process("FAILURE_PROD")[0]
    run(spark=spark,data=data,config=config)

    