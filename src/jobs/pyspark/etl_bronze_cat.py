from pyspark.sql import SparkSession

from readers import FactoryReader


def run(spark:SparkSession):
   
    config={
        'source':{
             'dbtable' : 'dw_nybike.bronze.trip_data_nybike',
             'reader' : 'icebergTable'
        }
    }
 
    df = FactoryReader().getDataframe(spark,config['source'])

    df.show(100)
    
       
   

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("IcebergNessieRead") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.demo.type", "rest") \
        .config("spark.sql.catalog.demo.uri", "http://rest:8181") \
        .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.demo.warehouse", "s3://warehouse/wh/") \
        .config("spark.sql.catalog.demo.s3.endpoint", "http://minio:9000") \
        .config("spark.sql.defaultCatalog", "demo") \
        .config("spark.eventLog.enabled","true") \
        .config("spark.eventLog.dir", "/home/iceberg/spark-events") \
        .config("spark.history.fs.logDirectory", "/home/iceberg/spark-events") \
        .config("spark.sql.catalogImplementation", "in-memory") \
        .getOrCreate()

    
    run(spark)
