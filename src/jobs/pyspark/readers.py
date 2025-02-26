from interfaces import Reader
from pyspark.sql import DataFrame

class ReaderCsvFromS3Storage(Reader):
    def run(self,spark,config:dict):
        return spark.read\
            .format('csv')\
                .options(inferSchema='True', header='True',delimiter=',')\
                    .load(config['path_csv'])

class ReaderCSV(Reader):
    def run(self,spark,config:dict)-> DataFrame:
        return spark.read\
            .format('csv')\
                .options(inferSchema='True', header='True',delimiter=',')\
                    .load(f"{config['root_path']}/{config['path_csv']}")
    
class ReaderDatabaseTable(Reader):
    def run(self,spark,config:dict)-> DataFrame:
        return spark.read\
            .format("jdbc") \
                .option("url", config['url']) \
                .option("dbtable",f'{config['schema']}.{config['dbtable']}')\
                .option("user", config['user']) \
                .option("password", config['password']) \
                .option("driver", config['driver']) \
                .option("fetchsize",1000) \
                    .load()
    
class ReaderDatabaseQuery(Reader):
    def run(self,spark,config:dict)-> DataFrame:
        query=config['query'].format(config['dw_period_tag'])
        # query=""" select * from bronze.trip_data_nybike where dw_period_tag='200401' """
        return spark.read\
            .format("jdbc") \
                .option("url", config['url']) \
                .option("query", query) \
                .option("user", config['user']) \
                .option("password", config['password']) \
                .option("driver", config['driver']) \
                .option("fetchsize",1000) \
                    .load()
    
class ReaderFromIceberg(Reader):
    def run(self,spark,config:dict) -> DataFrame:
         return spark.read \
            .format("iceberg") \
            .load(config['table'])

class FactoryReader:
    def getDataframe(self,spark,config,)->DataFrame:
        reader = config['reader']
        if reader=='ReaderCSVLocal':
            return ReaderCSV().run(spark,config)
        elif reader=='ReaderCsvFromS3Storage':
            return ReaderCsvFromS3Storage().run(spark,config)
        elif reader == 'database':
            return ReaderDatabaseTable().run(spark,config)
        elif reader == 'database_query':
            return ReaderDatabaseQuery().run(spark,config)
        elif reader == 'iceberg':
            return ReaderFromIceberg().run(spark,config)
        else:
            raise ValueError("Reader not found")


def getDataframeFromPsotgres(spark,config:dict):
    return spark.read.format("jdbc") \
    .option("url", config['url']) \
    .option("dbtable", config['dbtable']) \
    .option("user", config['db_user']) \
    .option("password", config['db_password']) \
    .option("driver", config['driver']) \
    .load()