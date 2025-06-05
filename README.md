# Project : NYC Bike Trips Lakehouse
## Overview
This is a project that aim to implement a datawarehouse that collection to aggregate and ploting some graph 

![Drag Racing](./docs_project/Datawarehouse_nybike_overview_drawio.png)

## Description of the Stak 

| Layer                      | Tools & Components                        | Key Role                                                 |
| -------------------------- | ----------------------------------------- | -------------------------------------------------------- |
| 1. Ingestion               | MinIO, Docker                             | Raw data landing zone                                    |
| 2. ETL & Transformation    | PySpark, Python                           | Medallion architecture pipeline (Bronze → Silver → Gold) |
| 3. Orchestration           | Apache Airflow, PostgreSQL                | Job scheduling, dependency management                    |
| 4. Storage & Versioning    | Apache Iceberg, Apache Nessie, PostgreSQL | ACID tables, schema evolution, time travel               |
| 5. Query                   | Dremio                                    | SQL interface over Iceberg tables                        |
| 6. Visualization           | Apache Superset                           | Dashboards and interactive analytics                     |
| 7. Infrastructure & DevOps | Docker Compose                            | Local deployment and development environment             |


## Dashbord on Superset
![Dashbord of trips](./docs_project/dashbord-trip-2025-06-04T11-17-49.781Z.jpg)


## Setup the docker compose 

```bash



```


## Command to submit jobs on different Layer or Stage

```bash
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
```


```bash
spark-submit \
--packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.102.5,software.amazon.awssdk:bundle:2.20.131,software.amazon.awssdk:url-connection-client:2.20.131" \
--conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions" \
--conf "spark.sql.catalog.warehouse=org.apache.iceberg.spark.SparkCatalog" \
--conf "spark.sql.catalog.warehouse.uri=$NESSIE_URI" \
--conf "spark.sql.catalog.warehouse.ref=main" \
--conf "spark.sql.catalog.warehouse.authentication.type=NONE" \
--conf "spark.sql.catalog.warehouse.catalog-impl=org.apache.iceberg.nessie.NessieCatalog" \
--conf "spark.sql.catalog.warehouse.s3.path-style-access=true" \
--conf "spark.sql.catalog.warehouse.s3.endpoint=$AWS_S3_ENDPOINT" \
--conf "spark.sql.catalog.warehouse.warehouse=$WAREHOUSE" \
--conf "spark.sql.catalog.warehouse.io-impl=org.apache.iceberg.aws.s3.S3FileIO" \
--conf "spark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY_ID" \
--conf "spark.hadoop.fs.s3a.secret.key=$AWS_SECRET_ACCESS_KEY" \
--conf "spark.hadoop.fs.s3a.path.style.access=true" \
--conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
--files ../../resoucrces/configs/config_etl_bronze_v2_iceberg.yaml --py-files interfaces.py,readers.py,sinkersType.py,etl_metadata.py,helpers_utils.py,transformers.py \
etl_bronze_nybike.py

```

```bash
spark-submit \
    --properties-file ../../resources/configs/config.conf \
    your_spark_job.py

command to lunch  sparks jobs :


etl bronze :

spark-submit --files ../../resources/configs/config_etl_1_v2.yaml --py-files interfaces.py,readers.py,sinkersType.py,etl_metadata.py,helpers_utils.py,transformers.py etl_bronze_nybike.py

spark-submit --files ../../resoucrces/configs/config_etl_1_v2_iceberg.yaml --py-files interfaces.py,readers.py,sinkersType.py,etl_metadata.py,helpers_utils.py,transformers.py etl_bronze_nybike.py

etl silver :

spark-submit \
--packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.102.5,software.amazon.awssdk:bundle:2.20.131,software.amazon.awssdk:url-connection-client:2.20.131" \
--conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions" \
--conf "spark.sql.catalog.warehouse=org.apache.iceberg.spark.SparkCatalog" \
--conf "spark.sql.catalog.warehouse.uri=$NESSIE_URI" \
--conf "spark.sql.catalog.warehouse.ref=main" \
--conf "spark.sql.catalog.warehouse.authentication.type=NONE" \
--conf "spark.sql.catalog.warehouse.catalog-impl=org.apache.iceberg.nessie.NessieCatalog" \
--conf "spark.sql.catalog.warehouse.s3.path-style-access=true" \
--conf "spark.sql.catalog.warehouse.s3.endpoint=$AWS_S3_ENDPOINT" \
--conf "spark.sql.catalog.warehouse.warehouse=$WAREHOUSE" \
--conf "spark.sql.catalog.warehouse.io-impl=org.apache.iceberg.aws.s3.S3FileIO" \
--conf "spark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY_ID" \
--conf "spark.hadoop.fs.s3a.secret.key=$AWS_SECRET_ACCESS_KEY" \
--conf "spark.hadoop.fs.s3a.path.style.access=true" \
--conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
--executor-memory 8g \
--driver-memory 8g \
--files ../../resoucrces/configs/config_etl_silver_v2_iceberg.yaml --py-files interfaces.py,readers.py,sinkersType.py,etl_metadata.py,helpers_utils.py,transformers.py etl_silver_nybike.py



spark-submit \
--packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.102.5,software.amazon.awssdk:bundle:2.20.131,software.amazon.awssdk:url-connection-client:2.20.131" \
--conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions"\
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
--executor-memory 8g \
--driver-memory 8g \
--files ../../resoucrces/configs/config_etl_2_v2_iceberg.yaml \
--py-files interfaces.py,readers.py,sinkersType.py,etl_metadata.py,helpers_utils.py,transformers.py etl_silver_nybike.py

spark-submit --files ../../resources/configs/config_etl_2_v2.yaml --py-files interfaces.py,readers.py,sinkersType.py,etl_metadata.py,helpers_utils.py,transformers.py etl_silver_nybike.py

spark-submit --files ../../resources/configs/config_etl_2_v2_iceberg.yaml --py-files interfaces.py,readers.py,sinkersType.py,etl_metadata.py,helpers_utils.py,transformers.py etl_silver_nybike.py



### etl gold

spark-submit \
--packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.102.5,software.amazon.awssdk:bundle:2.20.131,software.amazon.awssdk:url-connection-client:2.20.131" \
--conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions" \
--conf "spark.sql.catalog.warehouse=org.apache.iceberg.spark.SparkCatalog" \
--conf "spark.sql.catalog.warehouse.uri=$NESSIE_URI" \
--conf "spark.sql.catalog.warehouse.ref=main" \
--conf "spark.sql.catalog.warehouse.authentication.type=NONE" \
--conf "spark.sql.catalog.warehouse.catalog-impl=org.apache.iceberg.nessie.NessieCatalog" \
--conf "spark.sql.catalog.warehouse.s3.path-style-access=true" \
--conf "spark.sql.catalog.warehouse.s3.endpoint=$AWS_S3_ENDPOINT" \
--conf "spark.sql.catalog.warehouse.warehouse=$WAREHOUSE" \
--conf "spark.sql.catalog.warehouse.io-impl=org.apache.iceberg.aws.s3.S3FileIO" \
--conf "spark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY_ID" \
--conf "spark.hadoop.fs.s3a.secret.key=$AWS_SECRET_ACCESS_KEY" \
--conf "spark.hadoop.fs.s3a.path.style.access=true" \
--conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
--executor-memory 8g \
--driver-memory 8g \
--files ../../resoucrces/configs/config_etl_gold_v2_iceberg.yaml --py-files interfaces.py,readers.py,sinkersType.py,etl_metadata.py,helpers_utils.py,transformers.py,model_data.py etl_gold_stage.py




dremio+flight://benesh:3R!3peyVL8QHY@dremio:32010/?UseEncryption=false
dremio+flight://benesh:3R!3peyVL8QHY@dashboards:32010/?UseEncyption=false





Liens : de configuration de Nessie and Dremio 
https://www.linkedin.com/pulse/creating-local-data-lakehouse-using-alex-merced%3FtrackingId=owFrZg3DS7Ot0LnLS6Oz7A%253D%253D/?trackingId=owFrZg3DS7Ot0LnLS6Oz7A%3D%3D



setup airflow python path :
export PYTHONPATH="PYTHONPATH:(realpath src/jobs/pyspark)"

export PYTHONPATH="/path/to/jobs:/path/to/dags:$PYTHONPATH"




