from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import expr,count,col
from pyspark.sql import Row
from abc import ABC, abstractmethod
from pydantic import BaseModel


"""
list of data quality tests to implement
- Row count test
- Null value test on a given column
- Duplicate rows test
- Range test on a given column
- Pattern match test on a given column
"""

class BaseDataQualityTest(ABC):
    @abstractmethod
    def run(self,df:DataFrame, config:dict=None):
        pass


class RowCountDQ(BaseDataQualityTest):
    def __init__(self):
        self.name = "row_countdq"
    
    def interpret_test(self,result:bool,actual_rows:int,expected_rows:int):
        return {
            'dq_process_name': self.name,
            'expected_rows': expected_rows,
            'actual_rows': actual_rows,
            'status':result
        }

    def run(self,df:DataFrame,config:dict):
        actual_rows = df.count()
        result_bool = actual_rows == config['expected_rows_count'], f"Expected {config['expected_rows_count']} rows but got {actual_rows}"
        return self.interpret_test(result_bool,actual_rows=actual_rows,expected_rows=config['expected_rows_count'])

class NullValueDQ(BaseDataQualityTest):
    def __init__(self):
        self.name = "null_value_dq_test"
    
    def construct_result(self,result:bool,detail_result:dict,config:dict):
        return {
            'dq_process_name': self.name,
            'columns_checked': config['null_check']['column_list_check_null'],
            'status':result,
            'detail_result': detail_result
        }

    def run(self,df:DataFrame,config:dict):
        agg_count=0
        df_result_dict = {} #initialize result dict
        for c in config['null_check']['column_list_check_null']:
            agg_count += df.filter(col(c).isNull()).count() # check null count per column
            df_result_dict[c] = agg_count
        result_status = agg_count == 0
        return self.construct_result(result_status, df_result_dict, config)

class PatternMatchDQ(BaseDataQualityTest): 
    def __init__(self):
        self.name = "pattern_match_dq_test"

    def run(self,df:DataFrame, config:dict):
        mismatch_count = df.filter(~df[config['column_name']].rlike(config['pattern'])).count()
        result = mismatch_count == 0
        # return self.interpret_result(result)
        return result

factory_dq_tests = {
    "rows_count_check": RowCountDQ,
    "null_check": NullValueDQ,
    "pattern_match_check": PatternMatchDQ
}

class DataQualityTester(BaseModel):
    test_name: str
    config: dict

    @property
    def get_dq_tester(self):
        tester_class = factory_dq_tests.get(self.test_name)
        return tester_class()

def runner_data_quality_test(catalogue_data_quality:list[DataQualityTester], data:DataFrame):
    result_dq_test = []
    for element in catalogue_data_quality:
        dq_tester = element.get_dq_tester
        result_dq_test.append(dq_tester.run(data,element.config)) 
    return result_dq_test