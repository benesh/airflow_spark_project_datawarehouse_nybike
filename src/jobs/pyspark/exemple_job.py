# from pyspark.sql import SparkSession

from etl_metadata import ETL_Metadata,log_etl_metadata,get_data_to_process



def main():
    result = get_data_to_process()
    for data in result:
        print(data)


if __name__== "__main__":
    main()
    