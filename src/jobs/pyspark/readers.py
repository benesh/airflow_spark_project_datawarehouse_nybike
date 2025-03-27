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
        print("Reading CSV initiated") 
        return spark.read\
            .format('csv')\
                .options(inferSchema='True', header='True',delimiter=',')\
                    .load(f"{config['root_path']}/{config['path_csv']}")
    
class ReaderBulkCSV(Reader):
    def run(self,spark,config:dict)-> DataFrame:
        print("Reading CSV initiated From Path") 
        return spark.read.csv(config['list_csv'],header=True,inferSchema=False)
            # .format('csv')\
            #     .options(inferSchema='True', header='True',delimiter=',')\
            #         .load(f"{config['list_csv']}")
    
class ReaderDatabaseTable(Reader):
    def run(self,spark,config:dict)-> DataFrame:
        print("Reading table with table initiated")
        return spark.read\
            .format("jdbc") \
                .option("url", config['url']) \
                .option("dbtable",f'{config["schema"]}.{config["dbtable"]}')\
                .option("user", config['user']) \
                .option("password", config['password']) \
                .option("driver", config['driver']) \
                .option("fetchsize",20000) \
                    .load()
    
class ReaderDatabaseQuery(Reader):
    def run(self,spark,config:dict)-> DataFrame:
        print("Reading table with query initiated")
        query=config['query'].format(config['dw_period_tag'])
        # query=""" select * from bronze.trip_data_nybike where dw_period_tag='200401' """
        return spark.read\
            .format("jdbc") \
                .option("url", config['url']) \
                .option("query", query) \
                .option("user", config['user']) \
                .option("password", config['password']) \
                .option("driver", config['driver']) \
                .option("fetchsize",20000) \
                    .load()
    
class ReaderFromIceberg(Reader):
    def run(self,spark,config:dict) -> DataFrame:
         return spark.read \
            .format("iceberg") \
                .load(config['dbtable'])
    
class ReaderQueryFromIceberg(Reader):
    def run(self,spark,config:dict) -> DataFrame:
         return spark.sql(config['query'].format(config['dbtable'],config['dw_period_tag']))

class FactoryReader:
    def getDataframe(self,spark,config,)->DataFrame:
        reader = config['reader']
        if reader=='ReaderCSVLocal':
            return ReaderCSV().run(spark,config)
        elif reader=='ReaderBulkCSV':
            return ReaderBulkCSV().run(spark,config)
        elif reader=='ReaderCsvFromS3Storage':
            return ReaderCsvFromS3Storage().run(spark,config)
        elif reader == 'database':
            return ReaderDatabaseTable().run(spark,config)
        elif reader == 'database_query':
            return ReaderDatabaseQuery().run(spark,config)
        elif reader == 'icebergTable':
            return ReaderFromIceberg().run(spark,config)
        elif reader == 'IcebergQuery':
            return ReaderQueryFromIceberg().run(spark,config)
        else:
            raise ValueError("Reader not found")
