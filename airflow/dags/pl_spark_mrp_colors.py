# File: airflow/dags/pl_spark_mrp_colors.py

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='mrp_colors_csv_to_parquet',
    default_args=default_args,
    description='Запуск Spark-скрипта process_raw_trn.py из /opt/airflow/spark/scripts',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['spark','etl'],
) as dag:

    run_spark = SparkSubmitOperator(
        task_id='run_spark_csv2parquet123',
        conn_id='spark',                             # Spark connection в Admin → Connections
        application='/opt/airflow/spark/scripts/process_raw_trn.py',
        verbose=True,
        conf={
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
            'spark.hadoop.fs.s3a.access.key': 'minioadmin',
            'spark.hadoop.fs.s3a.secret.key': 'minioadmin',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
        },
    )

run_spark