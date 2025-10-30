from interfaces import SinkData
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


FACTORY_SINKER={
    'SinkDataToIceberg': SinkDataToIceberg(),
    'database': SinkDataToDatabase(),
    's3_system':SinkDataToParquetDirectory(),
    'merging_data': SinkDataPublishToMainBranch()
}

class FactorySinkData:
    def run(self,df:DataFrame,config:Optional[dict]):
        sink = config['sink']
        if sink == 'SinkDataToIceberg':
            return SinkDataToIceberg().run(df,config)
        elif sink == 'database':
            return SinkDataToDatabase().run(df,config)
        elif sink == 'file_parquet':
            return SinkDataToParquetDirectory().run(df,config)
        elif sink == 'merging_data':
            return SinkDataPublishToMainBranch().run(df,config)
        else :
            raise ValueError("sink not found")

def get_sinker(config:dict):
    return FACTORY_SINKER[config['sinker']]