import requests
import json
import pandas as pd
from boto3.s3.transfer import S3Transfer
import boto3
import datetime
from sqlalchemy import Table
from sqlalchemy.engine.base import Engine as sql_engine
import awswrangler as wr
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator 
from boto3.session import Session
from sqlalchemy import create_engine
from utils import instantiate_s3_client, instantiate_boto3_session, instantiate_sql_alchemy

DAG_ID = 'api_file-extract-and-upload'


default_args = {
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 7, 29),
    'retries': 3,
    'retry_delay': datetime.timedelta(seconds=10)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='subscription attributes data to the lake',
    tags=["extract_file", "upload_file"]
)

def extract_file():
    url = "http://universities.hipolabs.com/search?country=United+States"
    response = requests.get(url)
    data = response.text

    # converting string to JSON
    parsed = json.loads(data)
    json_data = print(json.dumps(parsed))
    df = pd.DataFrame({'col':parsed})

    # Save df  file to Paquet file format
    df_paquet = df.to_parquet('uni.parquet', index=False)


def upload_file_to_s3():
    client = boto3.client('s3')
    transfer = S3Transfer(client)
    transfer = S3Transfer(instantiate_s3_client('s3', Variable.get('ACCESS_KEY'), Variable.get('SECRET_KEY')))
    transfer.upload_file('uni.parquet', 'staging', 'uni.parquet', extra_args={'ServerSideEncryption': "AES256"})


def transfer_file():
    # read file using awswrangler
    df = wr.s3.read_parquet(path='s3://staging/uni.parquet', boto3_session=instantiate_boto3_session(Variable.get('ACCESS_KEY'), Variable.get('SECRET_KEY')))
    
    # transfer file from s3 to postgres rds db
    new_df = df.to_sql("uni-file", con=instantiate_sql_alchemy(), schema="olist")
    return new_df
    

extract_file = PythonOperator(
    dag=dag,
    task_id='extract_file',
    python_callable=extract_file
)

upload_file_to_s3 = PythonOperator(
    dag=dag,
    task_id='upload_file_to_s3',
    python_callable=upload_file_to_s3
)

transfer_file = PythonOperator(
    dag=dag,
    task_id='transfer_file',
    python_callable=transfer_file
)


extract_file >> upload_file_to_s3 >> transfer_file

