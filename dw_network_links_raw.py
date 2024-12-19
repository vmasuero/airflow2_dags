from datetime import datetime, timedelta
import pendulum
import os
import re

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.exceptions import AirflowException, AirflowFailException, AirflowSkipException
from airflow.decorators import dag, task


SFTP_CONNECTION = 'DevOpsBandWidth'
REMOTE_SFTP_PATH = '/files/traficoClaroVtr'
S3_PATH = 'NETWORK_COUNTERS/OYM'

SECRET_KEY ='2DhT3mGRLmNDBOl9ZuxCLdic0jXSmfUiZ+niJrwp3cU='
ACCESS_KEY = 'd7556c3cc7c1996477a5c851b51e2f47ea4d00a6'
REGION = 'sa-santiago-1'
NAMESPACE = 'axosppplfddw'
BUCKET = 'bucket-scl-prod-monitoreosscc-datalake-001'
ENDPOINT = "https://%s.compat.objectstorage.%s.oraclecloud.com"%(NAMESPACE,REGION)
bucket_url = f'https://objectstorage.{REGION}.oraclecloud.com/n/{NAMESPACE}/b/{BUCKET}/o/arieso/tmp/file_arieso.csv'



@task(
    executor_config={'LocalExecutor': {}},
)
def initialization(yesterday_ds = None, ds=None, ti=None, ds_nodash=None,  **kwargs):
    
    print("Yesteraday Date in RAW version: %s "%yesterday_ds)
    _date = datetime.strptime(str(yesterday_ds), "%Y-%m-%d")
    
    _year = _date.year
    _output_dir = "%s/%s"%(S3_PATH,_year)
    
    _file_ssh_traffic = '%s/%s_ClaroVtr_Traffic_v2.parquet'%(REMOTE_SFTP_PATH,ds_nodash)
    _file_shh_devifs = '%s/%s_ClaroVtr_Devifs.parquet'%(REMOTE_SFTP_PATH,ds_nodash)
    
    
    _file_s3_traffic = "%s/%s"%(_output_dir,_file_ssh_traffic.split('/')[-1])
    _file_s3_devifs = "%s/%s"%(_output_dir,_file_shh_devifs.split('/')[-1])
    
    ti.xcom_push(key='output_dir', value=_output_dir)
    ti.xcom_push(key='file_ssh_traffic', value=_file_ssh_traffic)
    ti.xcom_push(key='file_shh_devifs', value=_file_shh_devifs)
    ti.xcom_push(key='file_s3_traffic', value=_file_s3_traffic)
    ti.xcom_push(key='file_s3_devifs', value=_file_s3_devifs)
    
    return True



@task(
    executor_config={'LocalExecutor': {}},
)
def check_files(ti=None,  **kwargs):
    
    _file_ssh_traffic = ti.xcom_pull(task_ids='initialization', key='file_ssh_traffic') 
    _file_shh_devifs = ti.xcom_pull(task_ids='initialization', key='file_shh_devifs')
    
    conn = SFTPHook(ftp_conn_id=SFTP_CONNECTION)
    if not conn.isfile(_file_ssh_traffic):
        raise AirflowFailException("Remote file not exists: %s"%_file_ssh_traffic)
    print("File %s in server.....OK"%_file_ssh_traffic)
        
        
    if not conn.isfile(_file_shh_devifs):
        raise AirflowFailException("Remote file not exists: %s"%_file_shh_devifs)
    print("File %s in server.....OK"%_file_shh_devifs)
        

    return True

@task(
    executor_config={'LocalExecutor': {}},
)
def download_files(ti=None,  **kwargs):

    import tempfile
    import boto3
    
    _s3 = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION,
        endpoint_url=ENDPOINT
    )


    _file_ssh_traffic = ti.xcom_pull(task_ids='initialization', key='file_ssh_traffic') 
    _file_ssh_devifs = ti.xcom_pull(task_ids='initialization', key='file_shh_devifs')
    
    _file_s3_traffic = ti.xcom_pull(task_ids='initialization', key='file_s3_traffic') 
    _file_s3_devifs = ti.xcom_pull(task_ids='initialization', key='file_s3_devifs')
        

    _tmp_traffic = tempfile.NamedTemporaryFile()
    _tmp_devifs = tempfile.NamedTemporaryFile()    
    print("Created local file for traffic: %s"%_tmp_traffic.name)
    print("Created local file for devifs : %s"%_tmp_devifs.name)
    
    
    conn = SFTPHook(ftp_conn_id=SFTP_CONNECTION)
    try:
        print("Downloading file: %s"%_file_ssh_traffic)
        conn.retrieve_file( _file_ssh_traffic,_tmp_traffic.name)
    except FileNotFoundError as exp_enoent:
        raise AirflowFailException("Remote file not found: %s"%_file_ssh_traffic)

    try:
        print("Downloading file: %s"%_file_ssh_devifs)
        conn.retrieve_file(_file_ssh_devifs, _tmp_devifs.name)
    except FileNotFoundError as exp_enoent:
        raise AirflowFailException("Remote file not found: %s"%_file_ssh_devifs)


 
    print("Uploadig to: %s"%_file_s3_traffic)
    _s3.upload_file(_tmp_traffic.name, BUCKET, _file_s3_traffic)
    
    print("Uploading to: %s"%_file_s3_devifs)
    _s3.upload_file(_tmp_devifs.name, BUCKET, _file_s3_devifs)

    return True
    
with DAG(
    dag_id='dw_network_links_raw',
    schedule_interval= "30 9 * * *",
    default_args={
        "depends_on_past": False,
        "retries": 15,
        'max_active_runs': 1,
        "retry_delay": timedelta(minutes=30)
    },
    start_date=pendulum.datetime( 2024, 9, 1, tz='America/Santiago'),
    catchup=False,
    tags=['development', 'bw']
) as dag:

   
    initialization() >> check_files() >> download_files()