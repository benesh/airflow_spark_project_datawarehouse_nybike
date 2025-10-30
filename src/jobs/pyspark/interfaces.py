from abc import ABC,abstractmethod
from pyspark.sql import DataFrame
from typing import Optional

"""
Transformer of data for pyspark Dataframe 
"""
class DataTransformer(ABC):
    @abstractmethod
    def run(self,df:DataFrame,config:Optional[dict]) -> DataFrame:
        raise NotImplementedError

class SinkData(ABC):
    @abstractmethod
    def run(self,df:DataFrame,config:Optional[dict]):
        raise NotImplementedError

class ReadData(ABC):
    @abstractmethod
    def run(self,spark,config:Optional[dict])-> DataFrame:
        raise NotImplementedError

class ReportProcessor(ABC):
    def run (self,df:DataFrame,config:Optional[dict]):
        raise NotImplementedError

class TestBeforePublish(ABC):
    @abstractmethod
    def run(self,df:DataFrame,config:Optional[dict]) ->bool:
        raise NotImplementedError

class FileReader(ABC):
    @abstractmethod
    def run(self,config:Optional[dict]) -> dict:
        raise NotImplementedError
    

class ConnectDB(ABC):
    
    @abstractmethod
    def get_engine(self):
        raise NotImplementedError

    @abstractmethod
    def close_session(self,session):
        raise NotImplementedError

    @abstractmethod
    def close_engine(self):
        raise NotImplementedError