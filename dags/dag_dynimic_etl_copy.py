import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime,timedelta

# Set your variables here or use Airflow variables/connections
NESSIE_URI = "{{ var.value.NESSIE_URI }}"
AWS_S3_ENDPOINT = "{{ var.value.AWS_S3_ENDPOINT }}"
WAREHOUSE = "{{ var.value.WAREHOUSE }}"
AWS_ACCESS_KEY_ID = "{{ var.value.AWS_ACCESS_KEY_ID }}"
AWS_SECRET_ACCESS_KEY = "{{ var.value.AWS_SECRET_ACCESS_KEY }}"
CATALOG_NAME = "{{ var.value.CATALOG_NAME }}"
APPLICATION_FILE = "{{ var.value.APPLICATION_FILE }}"

 
configs = {
    'etl_bronze':{'application_file':'/opt/airflow/jobs/pyspark/etl_bronze_nybike.py', 'config_file':'/opt/airflow/resources/configs/config_etl_bronze_v2_iceberg.yaml'},
    'etl_silver':{'application_file':'/opt/airflow/jobs/pyspark/etl_silver_nybike.py','config_file':'/opt/airflow/resources/configs/config_etl_silver_v2_iceberg.yaml'},
    'etl_gold':{'application_file':'/opt/airflow/jobs/pyspark/etl_gold_stage.py','config_file':'/opt/airflow/resources/configs/config_etl_gold_v2_iceberg.yaml'}
}

def create_spark_dag(etl_name, config_jobs, param_default_args):
    with DAG(
        dag_id = f'dag_id_{etl_name}',
        default_args = param_default_args,
        schedule_interval =None,
        start_date = datetime(2023, 1, 1),
        catchup=False,
    ) as dag:
        python_job = SparkSubmitOperator(
        task_id = f"task_submit_{etl_name}",
        conn_id = "spark_conn",  # Set this up in Airflow Connections!
        application = config_jobs.get('application_file'),
        packages = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.102.5,software.amazon.awssdk:bundle:2.20.131,software.amazon.awssdk:url-connection-client:2.20.131",
        conf= {
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
            f"spark.sql.catalog.{CATALOG_NAME}": "org.apache.iceberg.spark.SparkCatalog",
            f"spark.sql.catalog.{CATALOG_NAME}.uri": NESSIE_URI,
            f"spark.sql.catalog.{CATALOG_NAME}.ref": "main",
            f"spark.sql.catalog.{CATALOG_NAME}.authentication.type": "NONE",
            f"spark.sql.catalog.{CATALOG_NAME}.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
            f"spark.sql.catalog.{CATALOG_NAME}.s3.path-style-access": "true",
            f"spark.sql.catalog.{CATALOG_NAME}.s3.endpoint": AWS_S3_ENDPOINT,
            f"spark.sql.catalog.{CATALOG_NAME}.warehouse": WAREHOUSE,
            f"spark.sql.catalog.{CATALOG_NAME}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "spark.hadoop.fs.s3a.access.key": AWS_ACCESS_KEY_ID,
            "spark.hadoop.fs.s3a.secret.key": AWS_SECRET_ACCESS_KEY,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.pyspark.python": "/usr/bin/python3.10",
            "spark.pyspark.driver.python": "/home/airflow/.local/bin/python3.12"
        },
        executor_memory = "4g",
        driver_memory = "4g",
        files = config_jobs.get('config_file'),
        py_files = '/opt/airflow/jobs/pyspark/interfaces.py,/opt/airflow/jobs/pyspark/readers.py,/opt/airflow/jobs/pyspark/sinkersType.py,/opt/airflow/jobs/pyspark/etl_metadata.py,/opt/airflow/jobs/pyspark/helpers_utils.py,/opt/airflow/jobs/pyspark/transformers.py,/opt/airflow/jobs/pyspark/model_data.py',
        dag = dag,
    )
    return dag


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

for etl_name, config_jobs in configs.items() :
    # dag_id = f"spark_job_{app_file.split('/')[-1].replace('.', '_')}"
    globals()[f'dag_id_{config_jobs.get('')}'] = create_spark_dag(etl_name, config_jobs, default_args)