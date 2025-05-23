# from geopy.distance import geodesic
import yaml
import os
from etl_metadata import Data_To_Process
from model_data import source_old_schema_ny_bike,source_actual_schema_ny_bike
# from pyspark.sql import SparkSession


def config_reader(path:str):
    # Load YAML config
    with open(path, "r") as f:
        config = yaml.safe_load(f)

    # Fetch secrets from environment variables
    # source_config = config["source"]
    # source_config["password"] = os.environ["SOURCE_DB_PASSWORD"]  # Inject password
    # etl_config = config['etl_conf']

    # dest_config = config["destination"]
    # dest_config["password"] = os.environ["DEST_DB_PASSWORD"]
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

