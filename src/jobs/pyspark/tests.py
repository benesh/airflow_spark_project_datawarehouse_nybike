from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import expr,count
from pyspark.sql import Row
from etl_metadata import ETL_Metadata
# def testNumberOfRows(,):

        
def test_number_of_rows_v2(df:DataFrame, expected_rows:int):
    assert df.count() == expected_rows, f"Expected {expected_rows} rows but got {df.count()}"