import csv
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

default_args = {
    'owner' : 'BOOK',
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

def fetch_from_postgres():
    # Connect to PostgreSQL
    hook = PostgresHook(postgres_conn_id='pg_container')

    #step 1 : query data form postgresq db
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM dbo.table_product_demand')
    data = cursor.fetchall()



    #download file to local
    csv_file_path = "dags/get_orders.csv"  # Specify the desired path for the exported file
    with open(csv_file_path, 'w') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerows(data)
    # Close the database connection
    cursor.close()
    conn.close()
    logging.info("Saved demand data")


     
     

def upload_to_minio():
    #step 2 : Store .csv file into S3
    s3_hook = S3Hook(aws_conn_id="minio")
    s3_hook.load_file(
        filename="dbo.table_product_demand",
        key="dbo.table_product_demand",
        bucket_name="datalake",
        replace=True
    )

with DAG(
        dag_id='hook_postgres_to_minio_V01',
        default_args=default_args,
        start_date=datetime(2023, 5, 23),
        schedule_interval='@daily'
    ) as dag:

        task1 = PythonOperator(
            task_id='fetch_from_postgres',
            python_callable=fetch_from_postgres
        )
        
        fetch_from_postgres