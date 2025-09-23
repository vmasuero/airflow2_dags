from datetime import datetime, timedelta
import pendulum
import os
import re
import boto3
import pandas as pd
from random import randint
from numpy import nan
import tempfile

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.exceptions import AirflowException, AirflowFailException, AirflowSkipException
from airflow.decorators import dag, task

from io import StringIO,BytesIO


SECRET_KEY = 'YVxIZIwFYvIGiv89xJuOuYyXubWtRomGF4EPre0Z'
ACCESS_KEY = 'READREPO_ACCESS_KEY'
ENDPOINT = 'http://10.68.12.60:9000'
BUCKET = 'readrepo'
PREFIX = 'traffic'



OCI_SECRET_KEY ='2DhT3mGRLmNDBOl9ZuxCLdic0jXSmfUiZ+niJrwp3cU='
OCI_ACCESS_KEY = 'd7556c3cc7c1996477a5c851b51e2f47ea4d00a6'
OCI_REGION = 'sa-santiago-1'
OCI_NAMESPACE = 'axosppplfddw'
OCI_BUCKET = 'bucket-scl-prod-monitoreosscc-datalake-001'
OCI_ENDPOINT = "https://%s.compat.objectstorage.%s.oraclecloud.com"%(OCI_NAMESPACE,OCI_REGION)


S3_PATH = 'NETWORK_COUNTERS/OYM_v3'
HEADERS_PATH = 'NETWORK_COUNTERS/HEADERS'
DIARY_REPORT_DIR = f'NETWORK_COUNTERS/REPORT_DIARY_v3/{int(datetime.now().strftime('%Y'))}'

def read_parquet_s3( s3_api, path:str, bucket:str, cols=[]):
    obj_buffer = s3_api.Object(bucket, path)

    with BytesIO(obj_buffer.get()['Body'].read()) as buffer_fd:
        
        if len(cols) > 0:
            _df = pd.read_parquet(buffer_fd, columns=cols)
        else:
            _df = pd.read_parquet(buffer_fd)

    return _df 
    
    

@task(
    executor_config={'LocalExecutor': {}},
)
def initialization(yesterday_ds = None, ds=None, ti=None, ds_nodash=None,  **kwargs):

    def get_last_version_file(file_list):
        versioned_files = []
        pattern = re.compile(r'network_headers_v(\d+)\.csv$')

        for f in file_list:
            match = pattern.search(f)
            if match:
                version = int(match.group(1))
                versioned_files.append((version, f))

        if not versioned_files:
            return None  # no matching files

        # return the file with the highest version
        return max(versioned_files, key=lambda x: x[0])[1]
    
    _s3_api = boto3.resource(
        's3',
        aws_access_key_id = OCI_ACCESS_KEY,
        aws_secret_access_key = OCI_SECRET_KEY,
        region_name = OCI_REGION, 
        endpoint_url = OCI_ENDPOINT
    )
    
    print("Yesteraday Date in RAW version: %s "%yesterday_ds)
    _date = datetime.strptime(str(yesterday_ds), "%Y-%m-%d")
    
    _year = _date.year
    _output_dir = "%s/%s"%(S3_PATH,_year)
    _remote_file = f"{PREFIX}/{ds_nodash}__ClaroVtr_Traffic_v3.parquet"
    
    
    #traffic/20250812_ClaroVtr_Traffic_v3.parquet
    
    _bucket = _s3_api.Bucket(OCI_BUCKET)
    _header_files = [obj.key for obj in _bucket.objects.filter(Prefix=HEADERS_PATH)]
    _header_file = get_last_version_file(_header_files)
    _header_file_prefix = _header_file.split('/')[-1].split('.')[0]
    
    
    _report_file_xls = f'{DIARY_REPORT_DIR}/{ds_nodash[:4]}-{ds_nodash[4:6]}-{ds_nodash[6:8]}_{_header_file_prefix}.xls'
    _report_file_parquet = f'{DIARY_REPORT_DIR}/{ds_nodash[:4]}-{ds_nodash[4:6]}-{ds_nodash[6:8]}_{_header_file_prefix}.parquet'

    
    
    ti.xcom_push(key='output_dir', value=_output_dir)
    ti.xcom_push(key='remote_file', value=_remote_file)
    ti.xcom_push(key='header_file', value=_header_file)
    ti.xcom_push(key='header_file_prefix', value=_header_file_prefix)
    ti.xcom_push(key='report_file_xls', value=_report_file_xls)
    ti.xcom_push(key='report_file_parquet', value=_report_file_parquet)

    return True


@task(
    executor_config={'LocalExecutor': {}},
)
def dowload_upload_raw(yesterday_ds = None, ds=None, ti=None, ds_nodash=None,  **kwargs):

    _remote_file = ti.xcom_pull(task_ids='initialization', key='remote_file') 
    _remote_file_oci = _remote_file.split('/')[-1]


    _s3_api_r = boto3.resource(
        's3',
        aws_access_key_id = ACCESS_KEY,
        aws_secret_access_key = SECRET_KEY,
        endpoint_url = ENDPOINT
    )

    _s3_api_oci = boto3.resource(
        's3',
        aws_access_key_id = OCI_ACCESS_KEY,
        aws_secret_access_key = OCI_SECRET_KEY,
        region_name = OCI_REGION, 
        endpoint_url = OCI_ENDPOINT
    )
    
    try:
        fd, tmp_path = tempfile.mkstemp()
        os.close(fd)
        
        print(f"Downlaod file: {_remote_file}")
        print(f"Temp file: {tmp_path}")
        _bucket = _s3_api_r.Bucket(BUCKET)
        _bucket.download_file(_remote_file, tmp_path)


        print(f"Upload file: {_remote_file_oci}")
        _bucket_oci = _s3_api_oci.Bucket(OCI_BUCKET)
        bucket.upload_file(tmp_path, _remote_file_oci, ExtraArgs={"ContentType": "application/x-parquet"})
    finally:
        os.remove(tmp_path) 
    
    return True
    
with DAG(
    dag_id='dw_network_links_raw_v3',
    schedule_interval= "30 9 * * *",
    default_args={
        "depends_on_past": False,
        'owner': 'Vmasuero',
        "retries": 15,
        'max_active_runs': 1,
        "retry_delay": timedelta(minutes=30)
    },
    start_date=pendulum.datetime( 2024, 9, 1, tz='America/Santiago'),
    catchup=False,
    tags=['development', 'bw']
) as dag:

   
    initialization() >> dowload_upload_raw()
    