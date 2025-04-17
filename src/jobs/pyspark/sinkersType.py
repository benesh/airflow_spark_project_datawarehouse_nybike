from interfaces import SinkData
from pyspark.sql import DataFrame 
from typing import Optional
# from pyspark.sql.functions import year,quarter,dayofweek,dayofmonth,month,lit,concat,date_format,count,expr,current_timestamp

class SinkDataToIceberg(SinkData):
    def run(self,df:DataFrame, config:Optional[dict]):
        print("Sink data to Iceberg")
        # try:
        df.write.saveAsTable(name=config['dbtable']
                             ,mode = config['mode']
                             )
        # except Exception as e:
        #     print(f"Error writing to {config['iceberg_table_name']}: {e}")
class SinkDataToParquetDirectory(SinkData):
    def run(self,df:DataFrame, config:Optional[dict]):
        print("Sink data to Iceberg")
        df.write\
            .partitionBy(config['column_partition'])\
                .mode(config['mode']).format("parquet")\
                    .save(path=config['path'])

class SinkDataToDatabase(SinkData):
    def run(self,df:DataFrame, config:Optional[dict]):
        # try:
        print("Sink data into database initated ")
        df.write.format("jdbc")\
            .option("url", config['url'])\
            .option("driver", config['driver'])\
            .option("dbtable",f'{config["schema"]}.{config["dbtable"]}')\
            .option("user", config['user'])\
            .option("password", config['password'])\
            .option("batchsize", 20000)\
                .mode(config['mode']).save()
        # except Exception as e:
            # print(f"Error writing to {config['dbtable']}: {e}")

class FactorySinkData:
    def run(self,df:DataFrame,config:Optional[dict]):
        sink = config['sink']
        if sink == 'SinkDataToIceberg':
            return SinkDataToIceberg().run(df,config)
        elif sink == 'database':
            return SinkDataToDatabase().run(df,config)
        elif sink == 'file_parquet':
            return SinkDataToParquetDirectory().run(df,config)
        else :
            raise ValueError("sink not found")
