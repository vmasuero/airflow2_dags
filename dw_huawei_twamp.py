from datetime import datetime, timedelta
import pendulum
import concurrent.futures
import pandas as pd
import os
import re
import boto3
from io import StringIO,BytesIO
import tempfile
import clickhouse_connect
from clickhouse_driver import Client

from airflow import DAG
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.exceptions import AirflowException, AirflowFailException, AirflowSkipException
from airflow.decorators import dag, task
from airflow.utils.helpers import chain


REMOTE_PATH = '/export/home/sysm/opt/oss/server/var/fileint'
HOURS_DELAY = 0

SECRET_KEY ='2DhT3mGRLmNDBOl9ZuxCLdic0jXSmfUiZ+niJrwp3cU='
ACCESS_KEY = 'd7556c3cc7c1996477a5c851b51e2f47ea4d00a6'
REGION = 'sa-santiago-1'
NAMESPACE = 'axosppplfddw'
BUCKET = 'bucket-scl-prod-monitoreosscc-datalake-001'
ENDPOINT = "https://%s.compat.objectstorage.%s.oraclecloud.com"%(NAMESPACE,REGION)


HUAWEI_FILES_PATH = 'Huawei/Twamps'


PM_HUAWEI_SERVERS = [
    'HUAWEI_CM_0',
    'HUAWEI_CM_1',
    'HUAWEI_CM_2',
    'HUAWEI_CM_3',
    'HUAWEI_CM_4',
    'HUAWEI_CM_5',
    'HUAWEI_CM_6',
    'HUAWEI_CM_7',
]

@task(
    executor_config={'LocalExecutor': {}},
)
def get_dates(yesterday_ds = None, ds=None, ti=None, data_interval_start=None,  **kwargs):
    #2024-01-07
    #huawei_twamp_v01_20240801124011_20241226060000DST.zip
    
    print("Yesteraday Date in RAW version: %s "%data_interval_start)
    _date_oper = data_interval_start-timedelta(hours=HOURS_DELAY)
    _hour_oper = _date_oper.hour
    
    
    _date_prefix_by_day = "%s%s%s"%(
        _date_oper.year,
        str(_date_oper.month).zfill(2),
        str(_date_oper.day).zfill(2)
    )
    
    _path_out_dir = "%s/%s/"%(
        HUAWEI_FILES_PATH,
        _date_oper.year
    )
    
      
    ti.xcom_push(key='date_prefix_by_day', value=_date_prefix_by_day)
    ti.xcom_push(key='path_out_dir', value=_path_out_dir)
    

    return True
 

@task(
    executor_config={'LocalExecutor': {}},
)
def download_files(ti=None, **kwargs):
    import re

    s3_api = boto3.resource('s3',
        aws_access_key_id = ACCESS_KEY,
        aws_secret_access_key = SECRET_KEY,
        region_name = REGION, 
        endpoint_url = ENDPOINT 
    )
    
   
    _date_prefix_by_day  = ti.xcom_pull(task_ids='get_dates', key='date_prefix_by_day')
    _path_out_dir = ti.xcom_pull(task_ids='get_dates', key='path_out_dir')
    _tmp_file = tempfile.NamedTemporaryFile(prefix='huawei_tmp_')
    

    print("Creando archivo temporal: %s"%_tmp_file.name)

    conn = SFTPHook(ftp_conn_id=PM_HUAWEI_SERVERS[0])
    _regex = r'huawei_twamp_v01_\d+_%s\d+DST\.zip'%_date_prefix_by_day
    print('Regex: %s'%_regex)
    
    _remote_files = conn.list_directory(REMOTE_PATH)
    _remote_files = [x for x in _remote_files if re.match(_regex,x)]
    
    if len(_remote_files) == 0:
        raise AirflowFailException('No existen archivo en directorio remoto')
       
    
    if len(_remote_files) != 1:
        raise AirflowFailException('Existen mas de un archivo: %s'%_remote_files)
        
    _remote_file = "%s/%s"%(REMOTE_PATH,_remote_files[0])
    _remote_file_s3 = "%s/%s"%(HUAWEI_FILES_PATH,_remote_files[0])
    
    print("Downloading file: %s  in tmp file: %s"%(_remote_file,_tmp_file.name))
    conn.retrieve_file(_remote_file, _tmp_file.name)
    
    print("Uploading file: %s  from tmp file: %s"%(_remote_file_s3,_tmp_file.name))
    s3_api.meta.client.upload_file(_tmp_file.name, BUCKET, _remote_file_s3)

    conn.close_conn()
    ti.xcom_push(key='remote_file_s3', value=_remote_file_s3)
    
    return True

@task(
    executor_config={'LocalExecutor': {}},
)
def upload_clickhouse(ti=None, **kwargs):
    import re

    s3_api = boto3.resource('s3',
        aws_access_key_id = ACCESS_KEY,
        aws_secret_access_key = SECRET_KEY,
        region_name = REGION, 
        endpoint_url = ENDPOINT 
    )
    
    bucket_cli = s3_api.Bucket(BUCKET)
   
    _remote_file_s3  = ti.xcom_pull(task_ids='download_files', key='remote_file_s3')
    _date_prefix_by_day  = ti.xcom_pull(task_ids='get_dates', key='date_prefix_by_day')

    _filter = HUAWEI_FILES_PATH + "/huawei_twamp_v01_"
    _regex = r'.*huawei_twamp_v01_\d+_%s\d+DST.zip'%_date_prefix_by_day

    
    FILES = [x.key for x in bucket_cli.objects.filter(Prefix=_filter)  if re.match(_regex,x.key)]
    
    print(FILES)

    print("Preparing the upload of file: %s"%_remote_file_s3)


    return True
  
with DAG(
    dag_id='dw_huawei_twamp',
    schedule_interval= "@daily",
    default_args={
        "depends_on_past": False,
        'owner': 'Vmasuero',
        "retries": 3,
        'max_active_runs': 1,
        "retry_delay": timedelta(minutes=5)
    },
    start_date=pendulum.datetime( 2024, 8, 3, tz='America/Santiago'),
    catchup=False,
    tags=['development', 'huawei','twamps']
) as dag:
 
    chain(
        get_dates(),
        download_files(),
        upload_clickhouse()
    )