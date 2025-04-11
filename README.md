# airflow_spark_project
ETL new york bike data transform and

base command 
`bash
spark-submit \
    --packages "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.1,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.3_2.12:0.67.0,software.amazon.awssdk:bundle:2.17.178,software.amazon.awssdk:url-connection-client:2.17.178" \
    --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
    --conf "spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog" \
    --conf "spark.sql.catalog.nessie.uri=http://nessie-server:19120/api/v1" \
    your_spark_job.py
`

spark-submit 
spark-submit \
    --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.102.5,software.amazon.awssdk:bundle:2.20.131,software.amazon.awssdk:url-connection-client:2.20.131" \
    --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions" \
    --conf "spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog" \
    --conf "spark.sql.catalog.nessie.uri=${NESSIE_URI}" \
    --conf "spark.sql.catalog.nessie.ref=main" \
    --conf "spark.sql.catalog.nessie.authentication.type=NONE" \
    --conf "spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog" \
    --conf "spark.sql.catalog.nessie.s3.endpoint=${AWS_S3_ENDPOINT}" \
    --conf "spark.sql.catalog.nessie.warehouse=${WAREHOUSE}" \
    --conf "spark.sql.catalog.nessie.io-impl=org.apache.iceberg.aws.s3.S3FileIO" \
    --conf "spark.sql.catalog.nessie.s3.path-style-access=true" \
    --conf "spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID}" \
    --conf "spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY}" \
    --files ../../resoucrces/configs/config_etl_1_v2_iceberg.yaml --py-files interfaces.py,readers.py,sinkersType.py,etl_metadata.py,helpers_utils.py,transformers.py etl_bronze_nybike.py


spark-submit \
--packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.102.5,software.amazon.awssdk:bundle:2.20.131,software.amazon.awssdk:url-connection-client:2.20.131" \
--conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions" \
--conf "spark.sql.catalog.bronze=org.apache.iceberg.spark.SparkCatalog" \
--conf "spark.sql.catalog.bronze.uri=$NESSIE_URI" \
--conf "spark.sql.catalog.bronze.ref=main" \
--conf "spark.sql.catalog.bronze.authentication.type=NONE" \
--conf "spark.sql.catalog.bronze.catalog-impl=org.apache.iceberg.nessie.NessieCatalog" \
--conf "spark.sql.catalog.bronze.s3.path-style-access=true" \
--conf "spark.sql.catalog.bronze.s3.endpoint=$AWS_S3_ENDPOINT" \
--conf "spark.sql.catalog.bronze.warehouse=$WAREHOUSE" \
--conf "spark.sql.catalog.bronze.io-impl=org.apache.iceberg.aws.s3.S3FileIO" \
--conf "spark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY_ID" \
--conf "spark.hadoop.fs.s3a.secret.key=$AWS_SECRET_ACCESS_KEY" \
--conf "spark.hadoop.fs.s3a.path.style.access=true" \
--conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
--files ../../resoucrces/configs/config_etl_1_v2_iceberg.yaml --py-files interfaces.py,readers.py,sinkersType.py,etl_metadata.py,helpers_utils.py,transformers.py etl_bronze_nybike.py



spark-submit \
    --properties-file ../../resources/configs/config.conf \
    your_spark_job.py

command to lunch  sparks jobs :


etl bronze :

spark-submit --files ../../resources/configs/config_etl_1_v2.yaml --py-files interfaces.py,readers.py,sinkersType.py,etl_metadata.py,helpers_utils.py,transformers.py etl_bronze_nybike.py

spark-submit --files ../../resoucrces/configs/config_etl_1_v2_iceberg.yaml --py-files interfaces.py,readers.py,sinkersType.py,etl_metadata.py,helpers_utils.py,transformers.py etl_bronze_nybike.py

etl sylver :

spark-submit --files ../../resources/configs/config_etl_2_v2.yaml --py-files interfaces.py,readers.py,sinkersType.py,etl_metadata.py,helpers_utils.py,transformers.py etl_sylver_nybike.py

spark-submit --files ../../resources/configs/config_etl_2_v2_iceberg.yaml --py-files interfaces.py,readers.py,sinkersType.py,etl_metadata.py,helpers_utils.py,transformers.py etl_sylver_nybike.py


spark submit version 2 with packages 


    # build: ./base_images_services/spark_notebook
