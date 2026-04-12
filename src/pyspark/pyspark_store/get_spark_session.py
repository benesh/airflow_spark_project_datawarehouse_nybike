from pyspark.sql import SparkSession



def get_spark_session(app_name: str,spark_conf:dict=None) -> SparkSession:
    
    builder = SparkSession.builder.appName(app_name)
     # Apply custom configurations if provided
    if spark_conf:
        for key, value in spark_conf.items():
            builder = builder.config(key, value)
    
    return builder.getOrCreate()
