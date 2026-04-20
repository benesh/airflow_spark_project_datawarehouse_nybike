from pyspark.src.interfaces import SinkData
from pyspark.sql import DataFrame,Column
from typing import Optional
# from pyspark.sql.functions import year,quarter,dayofweek,dayofmonth,month,lit,concat,date_format,count,expr,current_timestamp
from pyspark.sql.functions import col


class SinkDataToIceberg(SinkData):
    def run(self,df:DataFrame, config:Optional[dict]):
        print("Sink data to Iceberg")
        catalog_name = config['catalog_name']
        database = config['database']
        table = config["dbtable"]
        mode = config["mode"]
        target=f"{catalog_name}.{database}.{table}"
        if mode == "append":
            df.writeTo(target).append()
        elif mode == "overwrite":
            df.writeTo(target).overwrite(col( config['column_param_overwrite_name']) == config['column_param_overwrite_value'] )
        else:
            raise ValueError("Unsupported write mode for Iceberg table")
 
class SinkDataToParquetDirectory(SinkData):
    def run(self,df:DataFrame, config:Optional[dict]):
        print("Sink data to Iceberg")
        df.write\
            .partitionBy(config['column_partition'])\
                .mode(config['mode']).format("parquet")\
                    .save(path=config['path'])

class SinkDataToDatabase(SinkData):
    def run(self,df:DataFrame, config:Optional[dict]):
        print("Sink data into database initated ")
        df.write.format("jdbc")\
            .option("url", config['url'])\
            .option("driver", config['driver'])\
            .option("dbtable",f'{config["schema"]}.{config["dbtable"]}')\
            .option("user", config['user'])\
            .option("password", config['password'])\
            .option("batchsize", 20000)\
                .mode(config['mode']).save()
        
class SinkDataPublishToMainBranch(SinkData):
    def run(self,df:DataFrame, config:Optional[dict]):
        print("Merge data to main branch") 
        spark=df.sparkSession
        spark.sql(f"MERGE BRANCH {config['branch_source']} INTO {config['branch_target']} IN {config['catalog_name']}")

class SinkDataFrameToMultipleTables(SinkData):
    def run(self,df:DataFrame, config:Optional[dict]):
        print("Sink data to multiple tables")
        list_tables = config['list_tables_config']
        for table_config in list_tables:
            target=f"{config['catalog_name']}.{config['database']}.{table_config['dbtable']}"
            if config['mode'] == "append":
                df.select(*table_config['list_columns']).writeTo(target).append()
            elif config['mode'] == "overwrite":
                df.select(*table_config['list_columns']).writeTo(target).overwrite(col( table_config['column_param_overwrite_name']) == table_config['column_param_overwrite_value'] )
            else:
                raise ValueError("Unsupported write mode for Iceberg table")


FACTORY_SINKER={
    'SinkDataToIceberg': SinkDataToIceberg(),
    'database': SinkDataToDatabase(),
    's3_system':SinkDataToParquetDirectory(),
    'merging_data': SinkDataPublishToMainBranch(),
    'sink_data_to_multiple_tables':SinkDataFrameToMultipleTables()
}

def get_sinker(config:dict):
    return FACTORY_SINKER[config['sinker']]