# from geopy.distance import geodesic
import yaml
import os
# from etl_metadata import get_data_to_process,Data_To_Process,log_to_data_to_porcess


def config_reader(path:str):
    # Load YAML config
    with open(path, "r") as f:
        config = yaml.safe_load(f)
    return config

def list_files_with_format(directory,format_file):
    list_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(format_file):
                list_files.append(os.path.join(root, file))
    return list_files


def create_and_switch_to_branch(start_time,spark,config) -> str :
    ## create the name of
    string_date = str(start_time)
    string_date_str = string_date.replace(' ', '_').replace(':','_').replace('.','_').replace('-','_')
    branch_name=f"process_{config['etl_conf']['properties_data_etl']['process_name']}__{string_date_str}"
    # Create a new branch from main
    spark.sql(f"CREATE BRANCH IF NOT EXISTS {branch_name} IN {config['target']['catalog_name']} FROM main")
    # Switch to the new branch
    spark.sql(f"USE REFERENCE {branch_name} IN {config['target']['catalog_name']}")
    return branch_name