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

def instantiate_s3_client(s3, aws_access_key_id,aws_secret_access_key ):
    s3_client = boto3.client('s3', aws_access_key_id=Variable.get('ACCESS_KEY'), aws_secret_access_key=Variable.get('SECRET_KEY'))
    return s3_client


def instantiate_boto3_session(aws_access_key_id,aws_secret_access_key ):
    session = boto3.session.Session(aws_access_key_id=Variable.get('ACCESS_KEY'), 
                      aws_secret_access_key=Variable.get('SECRET_KEY')
                     )
    return session


def instantiate_sql_alchemy():
        conn = create_engine(Variable.get('postgres_rds_DB_credentials'))
        return conn

def dag_id(DAG_ID):
    DAG_ID = 'api_file-extract-and-upload11'
    return DAG_ID

def dag_authenticator(depends_on_past, start_date, retries, retry_delay):
     default_args = {
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 7, 29),
    'retries': 3,
    'retry_delay': datetime.timedelta(seconds=10)
     }
     return default_args



def dag(DAG_ID, default_args, description, tags):
    dag = DAG(
    DAG_ID='api_file-extract-and-upload11',
    default_args='default_args',
    description='subscription attributes data to the lake',
    tags=["extract_file", "upload_file"]
    )
    return dag



