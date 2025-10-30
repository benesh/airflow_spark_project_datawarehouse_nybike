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

