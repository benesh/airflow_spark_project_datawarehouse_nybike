from interfaces import SinkData
from pyspark.sql import DataFrame 
from typing import Optional
# from pyspark.sql.functions import year,quarter,dayofweek,dayofmonth,month,lit,concat,date_format,count,expr,current_timestamp

class SinkDataToIceberg(SinkData):
    def run(self,df:DataFrame, config:Optional[dict]):
        # try:
        df.write.mode(config['mode'])\
            .saveAsTable(config['iceberg_table_name'])
        # except Exception as e:
        #     print(f"Error writing to {config['iceberg_table_name']}: {e}")

class SinkDataToPostgres(SinkData):
    def run(self,df:DataFrame, config:Optional[dict]):
        # try:
        df.write.format("jdbc")\
            .option("url", config['url'])\
            .option("driver", config['driver'])\
            .option("dbtable", config['dbtable'])\
            .option("user", config['user'])\
            .option("password", config['password'])\
            .option("batchsize", 10000)\
                .mode(config['mode']).save()
        # except Exception as e:
            # print(f"Error writing to {config['dbtable']}: {e}")

class FactorySinkData:
    def run(self,df:DataFrame,config:Optional[dict]):
        sink = config['sink']
        if sink == 'SinkDataToIceberg':
            return SinkDataToIceberg().run(df,config)
        elif sink == 'database_table':
            return SinkDataToPostgres().run(df,config)
        else :
            raise ValueError("sink not found")
