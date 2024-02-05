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
        conn = create_engine("postgresql+psycopg2://staging_db:DnrbWdUcaZxyIc7v@postgres.cnelwn14hnqh.eu-central-1.rds.amazonaws.com:5432/main")
        return conn