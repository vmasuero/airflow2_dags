from datetime import datetime, timedelta
import pendulum
import os
import re
import boto3
import pandas as pd
from random import randint

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.exceptions import AirflowException, AirflowFailException, AirflowSkipException
from airflow.decorators import dag, task

from io import StringIO,BytesIO

import clickhouse_connect

SFTP_CONNECTION = 'DevOpsBandWidth'
REMOTE_SFTP_PATH = '/files/traficoClaroVtr'
S3_PATH = 'NETWORK_COUNTERS/OYM'
S3_PATH_DELTAS = 'NETWORK_COUNTERS/OYM_DELTAS'

SECRET_KEY ='2DhT3mGRLmNDBOl9ZuxCLdic0jXSmfUiZ+niJrwp3cU='
ACCESS_KEY = 'd7556c3cc7c1996477a5c851b51e2f47ea4d00a6'
REGION = 'sa-santiago-1'
NAMESPACE = 'axosppplfddw'
BUCKET = 'bucket-scl-prod-monitoreosscc-datalake-001'
ENDPOINT = "https://%s.compat.objectstorage.%s.oraclecloud.com"%(NAMESPACE,REGION)
bucket_url = f'https://objectstorage.{REGION}.oraclecloud.com/n/{NAMESPACE}/b/{BUCKET}/o/arieso/tmp/file_arieso.csv'



# CLICKHOUSE
SHARDS = 5
CLICKHOUSE_IP = 'clickhouse-counters.clickhouse.svc.cluster.local' 
CLICKHOUSE_IP_SHARDS = ['chi-counters-counters-%s-0.clickhouse.svc.cluster.local'%x for x in [x for x in range(SHARDS)]]

CLUSTER = 'counters'
CLICKHOUSE_PORT = 8123      
CLICKHOUSE_USERNAME = 'dev_user' 
CLICKHOUSE_PASSWORD = 'vtrclaro1234'      
DATABASE = 'OYM_COUNTERS' 
TABLE = 'BW'
TABLE_DIST = "%s_DIST"%TABLE
CLUSTER = 'counters'



def read_parquet_from_s3(path:str, s3_api):
    obj_buffer = s3_api.Object(BUCKET, path)
    
    with BytesIO(obj_buffer.get()['Body'].read()) as buffer:
        _df = pd.read_parquet(buffer)
        
    _df.reset_index(inplace=True)
    
    return _df
    
def read_parquet_s3( s3_api, path:str, bucket:str, cols=[]):
    obj_buffer = s3_api.Object(bucket, path)

    with BytesIO(obj_buffer.get()['Body'].read()) as buffer_fd:
        
        if len(cols) > 0:
            _df = pd.read_parquet(buffer_fd, columns=cols)
        else:
            _df = pd.read_parquet(buffer_fd)

    return _df 


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
    


def upload_parquet_s3(data:pd.DataFrame, filename:str, s3_api):

    _parquet_buffer = BytesIO()
    data.to_parquet(_parquet_buffer)
    
    _res = s3_api.Object(BUCKET, filename).put(Body=_parquet_buffer.getvalue())
    
    return list(_res.items())[0][1]['HTTPStatusCode'] == 200



@task(
    executor_config={'LocalExecutor': {}},
)
def initialization(yesterday_ds = None, ds=None, ti=None, ds_nodash=None,  **kwargs):
    
    _s3_api = boto3.resource(
        's3',
        aws_access_key_id = ACCESS_KEY,
        aws_secret_access_key = SECRET_KEY,
        region_name = REGION, 
        endpoint_url = ENDPOINT
    )

    
    print("Yesteraday Date in RAW version: %s "%yesterday_ds)
    _date = datetime.strptime(str(yesterday_ds), "%Y-%m-%d")
    
    _year = _date.year
    _output_dir = "%s/%s"%(S3_PATH,_year)
    
    
    _bucket = _s3_api.Bucket(BUCKET)
    _header_files = [obj.key for obj in _bucket.objects.filter(Prefix=HEADERS_PATH)]
    _header_file = get_last_version_file(_header_files)
    
    
    
    
    
    
    
    _file_ssh_traffic = '%s/%s_ClaroVtr_Traffic_v2.parquet'%(REMOTE_SFTP_PATH,ds_nodash)
    _file_shh_devifs = '%s/%s_ClaroVtr_Devifs.parquet'%(REMOTE_SFTP_PATH,ds_nodash)
    
    
    _file_s3_traffic = "%s/%s"%(_output_dir,_file_ssh_traffic.split('/')[-1])
    _file_s3_devifs = "%s/%s"%(_output_dir,_file_shh_devifs.split('/')[-1])
    
    ti.xcom_push(key='output_dir', value=_output_dir)
    ti.xcom_push(key='file_ssh_traffic', value=_file_ssh_traffic)
    ti.xcom_push(key='file_shh_devifs', value=_file_shh_devifs)
    ti.xcom_push(key='file_s3_traffic', value=_file_s3_traffic)
    ti.xcom_push(key='file_s3_devifs', value=_file_s3_devifs)
    ti.xcom_push(key='header_file', value=_header_file)

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
    

@task(
    executor_config={'LocalExecutor': {}},
    pool= 'CLICKHOUSE_POOL'
)
def upload_clickhouse(ti=None,  **kwargs):




    def filter_links(desc:str):
        _desc = desc.lower()

        _regular_exps = [
            r'link_[a-z]+_\d+.*',
            r'bbolt_.*',
            r'#ssip.*',
            r'#isp.*',
            r'#acceso.*',
            r'#ntwk.*',
            r'#mpls.*',
            r'#tv.*',
            r'#iptv.*',
            r'.*cache.*',
            r'.*pe\d+.*',
            r'.*ink_[a-z]+_\d+.*'
        ]
        
        for _reg in _regular_exps:          
            if re.match(_reg,_desc):
                return True   
      
        return False

    def proc_traffic(data:pd.DataFrame):

        MEGA = 1000000
        
        _traffic_df = data

        _traffic_df = _traffic_df[_traffic_df.ifadmin == 1]
        _traffic_df = _traffic_df[_traffic_df.ifoper == 1]
        
        _traffic_df['datetime'] = pd.to_datetime(_traffic_df['time'], format='%Y-%m-%d %H:%M:%S').dt.tz_localize('America/Santiago', ambiguous=True)

        _traffic_df['devif'] = pd.to_numeric(_traffic_df['devif'])
        _traffic_df['input'] = (pd.to_numeric(_traffic_df['input']) )
        _traffic_df['output'] = (pd.to_numeric(_traffic_df['output']) )
        
        _traffic_df.loc[_traffic_df['input'] < 0,'input'] = 0
        _traffic_df.loc[_traffic_df['output'] < 0,'output'] = 0
        
        _traffic_df = _traffic_df.sort_values(by='datetime')
        _traffic_df = _traffic_df[['devif','datetime','input','output']]

        _traffic_df.devif = _traffic_df.devif.astype('int32')
        _traffic_df.input = _traffic_df.input.astype('int64')
        _traffic_df.output = _traffic_df.output.astype('int64')
        _traffic_df['id'] = _traffic_df.devif.astype(str) + '-' + _traffic_df.datetime.dt.strftime('%Y%m%d%H%M%S')

        return _traffic_df
        
    

    _s3_api = boto3.resource('s3',
        aws_access_key_id = ACCESS_KEY,
        aws_secret_access_key = SECRET_KEY,
        region_name = REGION, 
        endpoint_url = ENDPOINT 
    )
    
    _client_chi_cli = clickhouse_connect.get_client(
        host = CLICKHOUSE_IP, 
        database = DATABASE, 
        user = CLICKHOUSE_USERNAME, 
        password = CLICKHOUSE_PASSWORD
    )
        
    _file_s3_traffic = ti.xcom_pull(task_ids='initialization', key='file_s3_traffic') 
    print(f"Processing file: {_file_s3_traffic}")
       
    _data_traffic = read_parquet_from_s3(_file_s3_traffic, _s3_api)
    _data_traffic = _data_traffic[ pd.notnull(_data_traffic.ifAlias)]
    _data_traffic = _data_traffic[ _data_traffic.ifAlias.apply(filter_links) ]
    _data_traffic = proc_traffic(_data_traffic)
       
    print(f"Uploading: {_data_traffic.shape[0]}")


    _sql_insert_df = "INSERT INTO %s.%s_DIST VALUES"%(DATABASE, TABLE)

    _n_chunks = int(_data_traffic.shape[0] / 10000)
    print(f"N CHUNKS: {_n_chunks}")

    _data_traffic['chunk'] = [randint(0,_n_chunks-1) for x in _data_traffic.index]
    
    for k,v in _data_traffic.groupby('chunk'):
        _client_chi_cli.insert_df(TABLE_DIST, v.drop('chunk', axis=1))
        
        if randint(0,20) == 5:
            print("Sending Chunk: %s"%k)
    
    print(f"Chunks uploaded: {k}")
    
    return True
    
@task(
    executor_config={'LocalExecutor': {}},
    pool= 'CLICKHOUSE_POOL'
)
def generate_deltas(ti=None,  **kwargs): 

    import boto3
    
    def filter_rows(data:pd.Series):
    
        _regular_exps = [
                    r'.*link_[a-z]+_\d+.*',
                    r'.*bbolt_.*',
                    r'#ssip.*',
                    r'#isp.*',
                    r'#acceso.*',
                    r'#ntwk.*',
                    r'#mpls.*',
                    r'#tv.*',
                    r'#iptv.*',
                    r'.*cache.*',
                    r'.*pe\d+.*',
                    r'.*ink_[a-z]+_\d+.*',
                ]
        
        _f = pd.Series()
        
        for _reg in _regular_exps:
            _f = _f | data.str.match(_reg) if len(_f) > 0 else data.str.match(_reg)

        return _f
    
    def read_df(s3_api, path, bucket):
        print(f"Reading Devices File {path}")
        _df = read_parquet_s3( s3_api, path, bucket)
        
        _df = _df[ _df.ifalias.apply(lambda x: len(x) > 20) ]
        _df = _df[ _df.ifadmin == 1]
        _df = _df[ _df.ifoper == 1]
        _df = _df[['operador','devname','ifname','ifalias','devif']]
        _df = _df[ filter_rows(_df.ifalias.str.lower()) ]
        _df = _df[ ~_df.ifname.str.match(r'.*\.\d+$')]
        _df.devname = _df.devname.str.split('.').apply(lambda x: x[0]) 
        _df = _df.set_index('devif')


        _path_traffic = path.replace('Devifs','Traffic_v2')
        print(f"Reading Traffic File {_path_traffic}")


        _df_cap = read_parquet_s3(s3_api, _path_traffic, bucket, cols=['devif','ifspeed'])
        _df_cap = _df_cap[['devif','ifspeed']].groupby('devif').max()/1000000
        _df_cap = _df_cap.rename(columns={'ifspeed':'capacidad_Gbps'})
        
        _df = _df.join(_df_cap, how='left')
        _df['date_f'] = pd.to_datetime( path.split('/')[-1].split('_')[0], format='%Y%m%d')
        
        return _df
    
    _s3 = boto3.resource('s3',
        aws_access_key_id = ACCESS_KEY,
        aws_secret_access_key = SECRET_KEY,
        region_name = REGION, 
        endpoint_url = ENDPOINT 
    )


    file_current_devif = ti.xcom_pull(task_ids='initialization', key='file_s3_devifs')  
    date_current_devif = pd.to_datetime( file_current_devif.split('/')[-1].split('_')[0], format='%Y%m%d')
    
    
    files = pd.DataFrame([x.key for x in  _s3.Bucket(BUCKET).objects.filter(Prefix=S3_PATH) if 'Devifs' in x.key]).rename(columns={0:'path'})
    files = files[files.path.str.match(r'.*\d+_ClaroVtr_Devifs.parquet$')]
    files['file'] = files.path.apply(lambda x: x.split('/')[-1] )
    files = files.join(files.file.str.extract(r'(\d\d\d\d)(\d\d)(\d\d)_ClaroVtr.*').rename(columns={0:'year',1:'month',2:'day'}).astype(int))
    files['date_f'] = files.apply(lambda x: datetime(x.year,x.month,x.day), axis=1)
    files = files.sort_values(by='date_f')
    files = files[ files.date_f < date_current_devif]
    files = files[ files.date_f == files.date_f.max()]
    
    flie_last_devif = files.path.iloc[0]
    
    print(f"Current File: {file_current_devif}")
    print(f"Last File: {flie_last_devif}")
    
    CURRENT_DF = read_df(_s3, file_current_devif, BUCKET )
    CURRENT_DF['status'] = 'NEW'
    
    LAST_DF = read_df(_s3, flie_last_devif, BUCKET )
    LAST_DF['status'] = 'DELETE'

    print('Copiling both files')
    COMP_DF = pd.concat([CURRENT_DF,LAST_DF])
    COMP_DF['check'] = COMP_DF.ifalias.str.replace(r'[^a-zA-Z0-9]', '', regex=True).str.lower()
    COMP_DF['repeated'] = COMP_DF.groupby(level=0)['check'].transform(lambda x: x.duplicated(keep=False))
    COMP_DF = COMP_DF[~COMP_DF.repeated]
    COMP_DF = COMP_DF.reset_index(drop=True)
   

    
    _file_s3_delta = S3_PATH_DELTAS +'/'+date_current_devif.strftime('%Y%m%d')+"_ClaroVtr_Devifs_Delta.parquet"
    print("Uploading to: %s"%_file_s3_delta)
    upload_parquet_s3(COMP_DF, _file_s3_delta, _s3)
    
    ti.xcom_push(key='file_s3_delta', value=_file_s3_delta)
    
    
    return True


    

        
with DAG(
    dag_id='dw_network_links_raw',
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

   
    initialization() >> check_files() #>> download_files() >> upload_clickhouse() >> generate_deltas()