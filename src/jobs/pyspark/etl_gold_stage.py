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
from model_data import silver_schema_ny_bike,ModelDatawahouseGoldNYBike


def run(spark:SparkSession,data_to_process:Data_To_Process,config:dict):
    try:
        # Step 1: Capture start time and initialize metadata
        start_time = datetime.now()
        metadata = ETL_Metadata(
            process_name="ETL_TO_GOLD_LAYER",
            start_time=start_time,
            status="IN_PROGRESS",
            process_period=data_to_process.process_period,
            year=data_to_process.year,
            month=data_to_process.month,
            data_to_process_id_fk=data_to_process.id
        )

        log_etl_metadata(metadata)  # Log initial metadata
        
        # catalog_transformer = [
        #     DataTransformerObject(
        #         transformer= FactoryDataTransformer.CAST_TO_TIMESTAMP,
        #         config=config['etl_conf']
        #     ),
        #     DataTransformerObject(
        #         transformer= FactoryDataTransformer.ADD_COLUMN_DIFF_TIME,
        #         config=config['etl_conf']['diff_column']
        #     ),
        #     DataTransformerObject(
        #         transformer= FactoryDataTransformer.GENDER_TRANSFORMER_OR_ADD,
        #         config={}
        #     )
        # ]


        df = FactoryReader().getDataframe(spark,config['source'])

        df_existing_rideable = spark.table("warehouse.gold.dim_rideable")

        """
            Create a new wbranch for securing the consistancy of the database if it fail 
            will cmerge to the main branch if success
        """
        
       ## create the name of 
        string_date = str(start_time)
        string_date_str = string_date.replace(' ', '_').replace(':','_').replace('.','_').replace('-','_')
        branch_name=f"feature_{metadata.process_name}__{string_date_str}"
        # Create a new branch from main
        spark.sql(f"CREATE BRANCH IF NOT EXISTS {branch_name} IN warehouse FROM main")

        ## transfrom process 
        # df = runner_transformer_data(catalog_transformer,df)

        # Switch to the new branch
        spark.sql(f"USE REFERENCE {branch_name} IN warehouse")

        df_1,config1 = ModelDatawahouseGoldNYBike().get_df_dim_customer(df,config['target'])
        FactorySinkData().run(df_1,config1)

        df_2,config2 = ModelDatawahouseGoldNYBike().get_df_fact_trip(df,config['target'])
        FactorySinkData().run(df_2,config2)

        df_3,config3 = ModelDatawahouseGoldNYBike().get_df_location(df,config['target'])
        FactorySinkData().run(df_3,config3)

        df_4,config4 = ModelDatawahouseGoldNYBike().get_df_dim_rideable(df,config['target'])
        df_4_to_insert = df_4.join( df_existing_rideable, on="enr_rideable_type_id", how="left_anti")
        FactorySinkData().run(df_4_to_insert,config4)

        df_5,config5 = ModelDatawahouseGoldNYBike().get_df_dim_time(df,config['target'])
        FactorySinkData().run(df_5,config5)

        # Merge the feature branch back into main
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

        data_to_process.status='FINAL_LAYER'
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

        data_to_process.status='FAILURE_TO_GOLD_LAYER'
        update_data_to_porcess(data_to_process)
        traceback.print_exc()
        print(f"ETL process failed: {e}")

if __name__ == "__main__":

    spark = SparkSession.builder \
    .appName("spark-etl_nybike_sylver") \
        .getOrCreate()

    path_file=SparkFiles.get("config_etl_gold_v2_iceberg.yaml")
    config = config_reader(path=path_file)

    data_to_process_list = get_row_to_process('FAILURE_TO_GOLD_LAYER','TO_GOLD_LAYER')
    print(data_to_process_list)
    if len(data_to_process_list) > 0 :
        data_to_process = data_to_process_list[0]    
        config['source']['dw_period_tag'] = data_to_process.period_tag 
        run(spark,data_to_process = data_to_process,config = config)
    else:
        print("No data available to precess in GOLD Layer")
    spark.stop()










