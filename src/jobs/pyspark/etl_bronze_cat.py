from pyspark.sql import SparkSession
import pyspark

# from readers import FactoryReader


def run(spark:SparkSession):
   
    # config={
    #     'source':{
    #          'dbtable' : 'dw_nybike.bronze.trip_data_nybike',
    #          'reader' : 'icebergTable'
    #     }
    # }
 
    # df = FactoryReader().getDataframe(spark,config['source'])

    # df.show(100)
    print(spark)
    
       
   

if __name__ == "__main__":
    NESSIE_URI ="http://nessie:19120/api/v1"
    WAREHOUSE = "s3a://warehouse/"
    AWS_ACCESS_KEY_ID = "SHvIFwr90PJRvwcFz1Ph"
    AWS_SECRET_ACCESS_KEY = "VKWwZK1LV8QCN8QbAINLVmxQCG96H7MC60KyzQbJ"
    AWS_S3_ENDPOINT = "http://minio:9000"
    # conf = (
    #     pyspark.SparkConf()
    #         .setAppName('app_name')
    #         .set('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.1,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.3_2.12:0.67.0,software.amazon.awssdk:bundle:2.17.178,software.amazon.awssdk:url-connection-client:2.17.178')
    #         .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions')
    #         .set('spark.sql.catalog.bronze', 'org.apache.iceberg.spark.SparkCatalog')
    #         .set('spark.sql.catalog.nessie.uri', NESSIE_URI)
    #         .set('spark.sql.catalog.nessie.ref', 'main')
    #         .set('spark.sql.catalog.nessie.authentication.type', 'NONE')
    #         .set('spark.sql.catalog.nessie.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog')
    #         .set('spark.sql.catalog.nessie.s3.endpoint', AWS_S3_ENDPOINT)
    #         .set('spark.sql.catalog.nessie.warehouse', WAREHOUSE)
    #         .set('spark.sql.catalog.nessie.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
    #         .set('spark.hadoop.fs.s3a.access.key', AWS_ACCESS_KEY_ID)
    #         .set('spark.hadoop.fs.s3a.secret.key', AWS_SECRET_ACCESS_KEY)
    # )

    ## Start Spark Session
    spark = SparkSession.builder.getOrCreate()
    # spark = SparkSession.builder.config(conf=conf).getOrCreate()

    

