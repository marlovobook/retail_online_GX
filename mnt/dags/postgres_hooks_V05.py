import csv
import logging
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator

default_args = {
    'owner' : 'BOOK',
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

def _fetch_from_postgres(ds_nodash, next_ds_nodash):
    # Connect to PostgreSQL
    hook = PostgresHook(postgres_conn_id='pg_container')

    #step 1 : query data form postgresq db
    conn = hook.get_conn()
    cursor = conn.cursor()
    # 1st %s = string of the start date (ds_nodash), 2nd %s = string = string of todaydate as interval =@Daily
    cursor.execute("SELECT * FROM dbo.table_product_demand WHERE date >=  %s and date < %s", (ds_nodash, next_ds_nodash))
    data = cursor.fetchall()



    #download file to local
    csv_file_path = f"dags/get_orders_{ds_nodash}.csv"  # Specify the desired path for the exported file
    with NamedTemporaryFile(mode='w', suffix=f"{ds_nodash}") as f:
    #with open(csv_file_path, 'w') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerows(data)
    # flush file object so the .csv is saved
        f.flush()    
    # Close the database connection
        cursor.close()
        conn.close()
        logging.info("Saved demand data: %s", csv_file_path)

    #step 2 : Store .csv file into S3

        s3_hook = S3Hook(aws_conn_id="minio")
        s3_hook.load_file(
            filename=f.name,
            #yyyy/mm/dd
            key=f"orders/{ds_nodash}.csv",
            bucket_name="datalake",
            replace=True
        )
        logging.info("Orders file %s has been pushed to S3!", f.name)
        logging.info("Orders file %s has been pushed to S3!", ds_nodash)

def parse_csv_to_list(filepath):

    with open(filepath, newline="") as file:
        return [row for row in csv.reader(file)]


with DAG(
        dag_id='hook_postgres_to_minio_V05',
        default_args=default_args,
        start_date=datetime(2023, 2, 18),
        schedule_interval='@daily'
    ) as dag:

        fetch_from_postgres = PythonOperator(
            task_id='fetch_from_postgres',
            python_callable=_fetch_from_postgres
        )

        s3_key = get_dynamic_s3_key
        transfer_s3_to_sql = S3ToSqlOperator(
            task_id="transfer_s3_to_sql",
            s3_bucket='datalake',
            s3_key=s3_key,
            table=SQL_TABLE_NAME,
            column_list=SQL_COLUMN_LIST,
            parser=parse_csv_to_list,
            sql_conn_id=conn_id_name,
        )

        
        fetch_from_postgres