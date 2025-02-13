from readers import FactoryReader
from sinkersType import FactorySinkData
from pyspark.sql import SparkSession
from tests import test_number_of_rows,test_number_of_rows_v2
from helpers_utils import config_reader
from datetime import datetime
from etl_metadata import ETL_Metadata,log_etl_metadata,Data_To_Process,get_data_to_process

def run(data:Data_To_Process,config:dict):
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

        # Create spark session
        spark = SparkSession.builder \
        .appName("spark-submit-ubuntu") \
        .getOrCreate()

        # Create reader factory
        reader = FactoryReader()
        
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
        
        # Get the rows report of data in stage database
        # config['report']['dbtable']='report_process'
        df_report= reader.getDataframe(spark,config['report'])
        df_report.createOrReplaceTempView('report_process')
        sql_query="SELECT * FROM report_process WHERE status='OK'"

        # get list of rows that is present in stage and not in prod stage
        list_report_in_stage = spark.sql(sql_query).collect()

        # sql_data_dim = f"SELECT * FROM {table_to_query} WHERE YEAR={year} AND month={mnth}"
        for report_row in list_report_in_stage:
            # Get year and month values from the report
            year = report_row['year_collected']
            month = report_row['month_collected']
            
            # Build WHERE clause conditionally
            month_condition = f"AND month={month}" if month > 0 else ""

            # test the number of rows
            df_date_result = spark.sql(f""" SELECT * FROM dim_date WHERE YEAR={report_row['year_collected']} {month_condition}""")
            test_number_of_rows_v2(df_date_result,report_row['number_of_rows'])
            df_station_result = spark.sql(f"""SELECT * FROM dim_station WHERE YEAR={report_row['year_collected']} {month_condition}""")
            test_number_of_rows_v2(df_date_result,report_row['number_of_rows'])
            df_fact_result = spark.sql(f"""SELECT * FROM dim_fact WHERE YEAR={report_row['year_collected']} {month_condition}""")
            test_number_of_rows_v2(df_date_result,report_row['number_of_rows'])

            """Write df to prod postgres"""
            config['url']='jdbc:postgresql://db:5432/prod_postgres'
            config['sink']='SinkDataToPostgres'
            # config['dbtable'] = 'fact_nybike'
            config['dbtable'] = 'dim_date_nybike'
            FactorySinkData().run(df_date_result,config)
            config['dbtable'] = 'dim_station_nybike'
            FactorySinkData().run(df_station_result,config)
            config['dbtable'] = 'fact_nybike'
            FactorySinkData().run(df_fact_result,config)

        # Step 3: Capture end time and update metadata
        end_time = datetime.now()
        rows_processed = df_date.count()
        duration = end_time - start_time

        metadata.end_time = end_time
        metadata.duration = duration
        metadata.rows_processed = rows_processed
        metadata.status = "SUCCESS"
        log_etl_metadata(metadata)  # Update metadata

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

        print(f"ETL process failed: {e}")

if __name__ == "__main__":
    path_config='../configs_job/config_etl_2.yaml'
    config = config_reader(path=path_config)
    result = get_data_to_process("TO_PROD_DATABASE")
   
    for data_to_process in result:
        config['source']['path_csv'] = data_to_process.path_csv
        config['source']['month'] = data_to_process.month
        config['source']['year'] = data_to_process.year
        config['source']['reader'] = data_to_process.reader 
        config['source']['process_period'] = data_to_process.process_period 

        run(data_to_process=data_to_process,config=config)



    
    # run(config=config)

    # config['process_period']='MONTHLY'
    # config['mode']='append'
    # run(path='/opt/airflow/include/2019-citibike-tripdata/11_November/*.csv',config=config)
 
    
    # config={
    #     'url':'jdbc:postgresql://db:5432/postgres',
    #     'db_user':'root',
    #     'db_password':'your-password',
    #     'mode':'overwrite',
    #     'reader':'PostgresDW',
    #     # 'reader':'iceberg',
    #     # 'query':'select * from public.fact_nybike',
    #     # 'table':'STAGE_NEWYORKCITYBIKE.FACT_NYBIKE',
    #     # 'dbtable':'fact_nybike',
    #     'driver':'org.postgresql.Driver',
    #     # 'mode':'append',
    #     'sink':'SinkDataToPostgres',
    #     # 'sink':'SinkDataToIceberg',
    #     'process_name':'ETL-NY-Process-Before-publish',
    #     'report_type':'ny_bike_report',
    #     'process_period':'ANNUAL'
    # }
    # run(config=config)