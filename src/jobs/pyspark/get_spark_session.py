from pyspark.sql import SparkSession





# df.write \
#   .format("jdbc") \
#   .option("url", "jdbc:mysql://host/db") \
#   .option("dbtable", "table_name") \
#   .option("user", "username") \
#   .option("password", "password") \
#   .option("batchsize", 1000) \
#   .mode("append") \
#   .save()


# from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName("Read S3 CSV").getOrCreate()

# hadoopConf = spark._jsc.hadoopConfiguration()
# hadoopConf.set("fs.s3a.endpoint", "http://<minio-host>:<minio-port>")
# hadoopConf.set("fs.s3a.access.key", "<minio-access-key>")
# hadoopConf.set("fs.s3a.secret.key", "<minio-secret-key>")
# hadoopConf.set("fs.s3a.path.style.access", "true")