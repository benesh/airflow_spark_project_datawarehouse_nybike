# from geopy.distance import geodesic
import yaml
import os
from etl_metadata import Data_To_Process
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


