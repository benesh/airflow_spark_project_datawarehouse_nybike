# from geopy.distance import geodesic
import yaml
import os
from etl_metadata import Data_To_Process
from model_data import source_old_schema_ny_bike,source_actual_schema_ny_bike
# from pyspark.sql import SparkSession
from etl_metadata import ETL_Metadata,log_etl_metadata,get_data_to_process,Data_To_Process,update_data_to_porcess


def config_reader(path:str):
    # Load YAML config
    with open(path, "r") as f:
        config = yaml.safe_load(f)
    return config


def get_schema(config:dict,year:int):
    if year < 2020:
        config['etl_conf']['schema'] = source_old_schema_ny_bike
    else:
        config['source']['schema'] = source_actual_schema_ny_bike
    return config

def list_files_with_format(directory,format_file):
    list_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(format_file):
                list_files.append(os.path.join(root, file))
    return list_files

def get_row_to_process(retry_status:str,new_data_status):
    """
    Get the right row to process.
    Read the database, verifying if there isn't a row to rerun or retry and then continue with the new data to process 
    """
    result = get_data_to_process(retry_status)
    if len(result) > 0:
        return result[0] 
    result = get_data_to_process(new_data_status)
    return result[0]