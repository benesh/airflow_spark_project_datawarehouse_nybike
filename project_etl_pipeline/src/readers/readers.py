from interfaces import ReadData
from pyspark.sql import DataFrame
from settings import PATH_FILES


class ReaderCsvFromS3Storage(ReadData):
    def run(self,spark,config:dict):
        return spark.read\
            .csv(config[PATH_FILES],header=True)

class ReaderCSV(ReadData):
    def run(self,spark,config:dict)-> DataFrame:
        print("Reading CSV initiated") 
        return spark.read\
            .format('csv')\
                .options(header='True')\
                    .load(f"{config['root_path']}/{config['path_csv']}")

class ReaderBulkCSV(ReadData):
    def run(self,spark,config:dict)-> DataFrame:
        print("Reading CSV initiated From Path") 
        return spark.read.csv(config['list_csv'],header=True)
    
class ReaderDatabaseTable(ReadData):
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
    
class ReaderDatabaseQuery(ReadData):
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
    
class ReaderFromIceberg(ReadData):
    def run(self,spark,config:dict) -> DataFrame:
         return spark.read \
            .format("iceberg") \
                .load(config['dbtable'])
    
class ReaderQueryFromIceberg(ReadData):
    def run(self,spark,config:dict) -> DataFrame:
        #  print(config['query'].format(config['dbtable'],config['dw_period_tag']))
         return spark.sql(config['query'].format(config['dbtable'],config['dw_period_tag']))

class ReaderFromIcebergWithPartition(ReadData):
    def run(self,spark,config:dict) -> DataFrame:
         return spark.table(f"{config['catalog_name']}.{config['database']}.{config['dbtable']}")\
            .filter(f"{config['column_partition']} = '{config['value_partition']}'")


FACTORY_READER={
    'ReaderCSVLocal':ReaderCSV(),
    'ReaderBulkCSV':ReaderBulkCSV(),
    'ReaderCsvFromS3Storage':ReaderCsvFromS3Storage(),
    'database':ReaderDatabaseTable(),
    'database_query':ReaderDatabaseQuery(), 
    'icebergTable':ReaderFromIceberg(),
    'IcebergQuery':ReaderQueryFromIceberg(),
    'iceberg_with_partition':ReaderFromIcebergWithPartition()
}

def get_reader(config) -> ReadData :
    reader_data :ReadData = FACTORY_READER[config['reader']]
    return reader_data