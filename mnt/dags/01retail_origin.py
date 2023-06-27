import os
import logging
import csv
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile


from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.dummy import DummyOperator


# Function for loading .csv from local to Postgres
def _load_data():

    postgres_hook = PostgresHook(postgres_conn_id='pg_container')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    
    #Path in local
    file_name = 'dags/temp/online_retail_origin.csv'

    postgres_hook.copy_expert(

        """
            COPY 
                dbo.table_online_retail_origin(id, Invoice, StockCode, Description,Quantity,InvoiceDate,Price,Customer_ID,Country,last_updated)
            
            FROM STDIN DELIMITER ',' CSV HEADER

        """,
        file_name,

    )



default_args = {
    'owner' : 'BOOK',
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'catchup' : False,
    ######## w8 previous task ##########
    'wait_for_downstream' : True,
    'depends_on_past':True,
    ######## w8 previous task ##########
    'catchup' : False, 
}
# Define your DAG
with DAG(
    dag_id='01_retail_origin',
    default_args=default_args,
    description='Incrementally Copy online_retail_origin file from local',
    schedule_interval="@daily",  # Set your desired schedule interval '@daily'
    start_date=datetime(2009, 12, 1),  # Set the start date of the DAG
    end_date=datetime(2009, 12, 6)# end

)as dags:
    
    start = DummyOperator(task_id="start")


    #Create Table in postgres
    # upload_retail_origin = PostgresOperator(
    #     task_id='create_online_retail_origin_in_data_warehouse',
    #     postgres_conn_id="pg_container",
    #     sql=f"""
    #         DROP TABLE IF EXISTS wh.table_online_retail_origin;

            

    #         CREATE TABLE wh.table_online_retail_origin (
    #             id INT,
    #             Invoice VARCHAR(100),
    #             StockCode VARCHAR(100),
    #             Description VARCHAR(100),
    #             Quantity INT,
    #             InvoiceDate TIMESTAMP,
    #             Price FLOAT,
    #             Customer_ID VARCHAR(100),
    #             Country VARCHAR(100),
    #             last_updated TIMESTAMP,
    #             constraint table_online_retail_origin_pk primary key (id)
    #         );

            
        
    #     """,
    # )
    
    #Call the _load_data function
    
    # load_data = PythonOperator(
    #     task_id="load_data",
    #     python_callable=_load_data,
    # )
    #DROP TABLE IF EXISTS wh.table_online_retail_origin;

    create_retail_origin = PostgresOperator(
        task_id='create_online_retail_origin_in_data_warehouse',
        postgres_conn_id="pg_container",
        sql=f"""
            

            CREATE TABLE IF NOT EXISTS wh.table_online_retail_origin (
                id INT,
                Invoice VARCHAR(100),
                StockCode VARCHAR(100),
                Description VARCHAR(100),
                Quantity INT,
                InvoiceDate TIMESTAMP,
                Price FLOAT,
                Customer_ID VARCHAR(100),
                Country VARCHAR(100),
                last_updated TIMESTAMP,
                operation char(1),
                constraint table_online_retail_stage_pk primary key (id, last_updated)
            );

        """,
    )

    insert_original_data = PostgresOperator(
        task_id="insert_original_data",
        postgres_conn_id="pg_container",
        sql=f"""

            INSERT INTO wh.table_online_retail_origin (
                id,
                Invoice,
                StockCode,
                Description,
                Quantity,
                InvoiceDate,
                Price,
                Customer_ID,
                Country,
                last_updated,
                operation
            )
            SELECT
                id,
                Invoice,
                StockCode,
                Description,
                Quantity,
                InvoiceDate,
                Price,
                Customer_ID,
                Country,
                last_updated,
                operation
            FROM
                dbo.table_online_retail_origin

            WHERE
                InvoiceDate >= '{{{{ds}}}}' AND InvoiceDate < '{{{{next_ds}}}}'
        """,
    )

    to_datalake = DummyOperator(task_id='to_lake')

    end = DummyOperator(task_id='end')

    # Set task dependenciessssdd
    start >> create_retail_origin >> insert_original_data >> to_datalake >> end
    