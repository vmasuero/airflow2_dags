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
from random import randint

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

TWAMP_COLS = [
    'Date',
    'Time',
    'eNodeB Name',
    'Sender Index',
    'VS.BSTWAMP.MaxRttDelay(ms)',
    'VS.BSTWAMP.MinRttDelay(ms)',
    'VS.BSTWAMP.Rtt.Means(ms)',
    'VS.BSTWAMP.Sender.RxPackets(packet)',
    'VS.BSTWAMP.Sender.TxPackets(packet)'
]

TWAMP_COLS_RENAME = [
    'Date',
    'Time',
    'ENodeB',
    'Twamp_id',
    'MaxRttDelay(ms)',
    'MinRttDelay(ms)',
    'Rtt.Means(ms)',
    'RxPackets(packet)',
    'TxPackets(packet)'
]

TWAMP_COLS_REPORT = [
    'DateTime',
    'ENodeB',
    'Twamp_id',
    'MaxRttDelay(ms)',
    'MinRttDelay(ms)',
    'Rtt.Means(ms)',
    'RxPackets(packet)',
    'TxPackets(packet)'
]


TWAMP_COLS_NUMBERS = [
    'MaxRttDelay(ms)',
    'MinRttDelay(ms)',
    'Rtt.Means(ms)',
    'RxPackets(packet)',
    'TxPackets(packet)'
]


CLICKHOUSE_IP = 'clickhouse-counters.clickhouse.svc.cluster.local' 
N_SHARDS = 5
SHARDS = [x for x in range(N_SHARDS)]
CLICKHOUSE_IP_SHARDS = ['chi-counters-counters-%s-0.clickhouse.svc.cluster.local'%x for x in SHARDS]

CLUSTER = 'counters'
CLICKHOUSE_PORT = 8123      
CLICKHOUSE_USERNAME = 'dev_user' 
CLICKHOUSE_PASSWORD = 'vtrclaro1234'      

DATABASE = 'TWAMP'
TABLE='COUNTERS'
TABLE_DIST = "%s_DIST"%TABLE

SQL_INSERT_DF = '''
INSERT INTO %s.%s VALUES
'''%(DATABASE, TABLE_DIST)

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
    _regex = r'huawei_twamp_v01_\d+_%s\d+(DST)?\.zip'%_date_prefix_by_day
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


    
    def read_zip_twamp_s3(path:str, s3_api):
        obj_buffer = s3_api.Object(BUCKET, path)

        with BytesIO(obj_buffer.get()['Body'].read()) as buffer:
            _df = pd.read_csv(buffer, compression='zip', skiprows=5, usecols=TWAMP_COLS)
        
        return _df
        
    def read_twamp_data(path:str, s3_api):

        _data = read_zip_twamp_s3(path, s3_api)

        _data = _data[TWAMP_COLS]
        _data.columns = TWAMP_COLS_RENAME
        print(_data.shape)
        _data = _data[pd.notnull(_data).all(axis=1)]

        

        _data.Date = _data.Date.str.replace(' DST','')
        _data.Time = _data.Time.str.replace(' DST','')
        _data = _data[_data.Date.str.match(r'\d\d\d\d-\d\d-\d\d')]
    
        _data.Time = _data.Time.apply(lambda x: datetime.strptime(x, "%H:%M").time() )
        _data['DateTime'] = pd.to_datetime(_data['Date'].astype(str) + ' ' + _data['Time'].astype(str))
        
        
        
        for _col in TWAMP_COLS_NUMBERS:
            print(_col)
            _data[_col] = pd.to_numeric(_data[_col], errors='coerce')
        _data = _data[pd.notnull(_data).all(axis=1)]


        
        _data['Twamp_id'] = _data['Twamp_id'].astype('int8')
        _data['MaxRttDelay(ms)'] = _data['MaxRttDelay(ms)'].astype('int32')
        _data['MinRttDelay(ms)'] = _data['MinRttDelay(ms)'].astype('int32')
        _data['Rtt.Means(ms)'] = _data['Rtt.Means(ms)'].astype('int32')
        _data['RxPackets(packet)'] = _data['RxPackets(packet)'].astype('int16')
        _data['TxPackets(packet)'] = _data['TxPackets(packet)'].astype('int16')

        
        _data = _data[TWAMP_COLS_REPORT]
        
        # PATH , se eliminan los parentesis de los nombres de las columnas
        _data = _data.rename(columns={
            'MaxRttDelay(ms)':'MaxRttDelay',
            'MinRttDelay(ms)':'MinRttDelay',
            'Rtt.Means(ms)':'RttMeans',
            'RxPackets(packet)':'RxPackets',
            'TxPackets(packet)':'TxPackets',
        })
        
        return _data

    s3_api = boto3.resource('s3',
        aws_access_key_id = ACCESS_KEY,
        aws_secret_access_key = SECRET_KEY,
        region_name = REGION, 
        endpoint_url = ENDPOINT 
    )
    
    client_ch_cli = Client(
        host = CLICKHOUSE_IP, 
        database = DATABASE, 
        user = CLICKHOUSE_USERNAME, 
        password = CLICKHOUSE_PASSWORD
    )
    
    bucket_cli = s3_api.Bucket(BUCKET)
   
    _date_prefix_by_day  = ti.xcom_pull(task_ids='get_dates', key='date_prefix_by_day')

    _filter = HUAWEI_FILES_PATH + "/huawei_twamp_v01_"
    _regex = r'.*huawei_twamp_v01_\d+_%s\d+(DST)?.zip'%_date_prefix_by_day

    
    FILES = [x.key for x in bucket_cli.objects.filter(Prefix=_filter)  if re.match(_regex,x.key)]
    print('FILES:')
    print(FILES)

    
    if len(FILES) == 0:
        raise AirflowFailException('No se encuentra el archivo %s'%_regex)
        
    if len(FILES) > 1:
        raise AirflowFailException('Se encuentra mas de un archivo %s'%FILES)
    
    _remote_file_s3 = FILES[0]

    print("Preparing the upload of file: %s"%_remote_file_s3)
    
    _twamp_data = read_twamp_data(_remote_file_s3, s3_api)

    _n_chunks = int(_twamp_data.shape[0] / 10000)
    _twamp_data['chunk'] = [randint(0,_n_chunks-1) for x in _twamp_data.index]

    print(SQL_INSERT_DF)
    
    print(_twamp_data.sample(10))
    
    for k,v in _twamp_data.groupby('chunk'):
        print("Sending Chunk: %s"%k)
        client_ch_cli.insert_dataframe(SQL_INSERT_DF, v.drop('chunk', axis=1), settings=dict(use_numpy=True))

    
    return True
    
    
@task(
    executor_config={'LocalExecutor': {}},
)
def delete_older_files(ti=None, **kwargs):

    conn = SFTPHook(ftp_conn_id=PM_HUAWEI_SERVERS[0])
    _regex = r'huawei_twamp_v01_\d+_\d+(DST)?\.zip'
    print('Regex: %s'%_regex)
    
    _remote_files = conn.list_directory(REMOTE_PATH)
 
    _remote_files = [x for x in _remote_files if re.match(_regex,x)]
    
    if len(_remote_files) == 0:
        raise AirflowFailException('No existen archivo en directorio remoto')
        
    print(_remote_files)
    
    return True
  
with DAG(
    dag_id='dw_huawei_twamp',
    schedule_interval= "0 5 * * *",
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
        #upload_clickhouse()
        delete_older_files()
    )
