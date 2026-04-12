from types import NoneType
from pyspark.sql import SparkSession, DataFrame
from interfaces import ReadData,SinkData,DataTransformer
from readers import get_reader
from sinkers import get_sinker
from data_quality import runner_data_quality_test,BaseDataQualityTest
from transformers import runner_transformer_data
from typing import Optional

class StepsPipelinesEtl:

    def __init__(self, spark:SparkSession, catalog_transformer:list[DataTransformer]= None, config:dict=None):
        self.spark = spark
        self.read_data = get_reader(config=config['source'])
        self.sink_data = get_sinker(config=config['target'])
        self.catalog_transformer = catalog_transformer
        self.config = config

    def run(self) -> int:
        # Step 1: Read data
        df:DataFrame = self.read_data.run(self.spark,self.config['source'])
        # Step 2: Transform data

        if self.catalog_transformer is not type(None):
            df = runner_transformer_data(self.catalog_transformer,df)
        # Step 3: Sink data
        self.sink_data.run(df,self.config['target'])
        # Step 4: Return number of rows processed
        return df.count()

class StepsPipelinesDataQuality:

    def __init__(self, spark:SparkSession, catalog_data_quality:list[BaseDataQualityTest], config:dict ):
        self.spark = spark
        self.read_data : ReadData = get_reader(config=config['source'])
        self.catalog_data_quality = catalog_data_quality
        self.sink_data = get_sinker(config=config['target'])
        self.config = config

    def run(self) -> dict:

        # Step 1: Read data
        df:DataFrame = self.read_data.run(self.spark,self.config['source'])
        # Step 2: Apply data quality tests
        result = runner_data_quality_test(self.catalog_data_quality,df)
        # Step 3: Merge if data quality tests passed
        if all(r['status'] for r in result):
            self.sink_data.run(df,self.config['target'])
        return result

