from datetime import datetime, timedelta
import pendulum
import concurrent.futures
import pandas as pd
import os
import re
import boto3
from io import StringIO,BytesIO
import tempfile

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.exceptions import AirflowException, AirflowFailException, AirflowSkipException
from airflow.decorators import dag, task
from kubernetes.client import models as k8s
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.helpers import chain

from dask.distributed import Client, LocalCluster, Future, as_completed, fire_and_forget


from redis import Redis



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

REMOTE_PATH = '/export/home/sysm/opt/oss/server/var/fileint/pm'
HOURS_DELAY = 12

SECRET_KEY ='2DhT3mGRLmNDBOl9ZuxCLdic0jXSmfUiZ+niJrwp3cU='
ACCESS_KEY = 'd7556c3cc7c1996477a5c851b51e2f47ea4d00a6'
REGION = 'sa-santiago-1'
NAMESPACE = 'axosppplfddw'
BUCKET = 'bucket-scl-prod-monitoreosscc-datalake-001'
ENDPOINT = "https://%s.compat.objectstorage.%s.oraclecloud.com"%(NAMESPACE,REGION)
URL_PRESHARED = "https://objectstorage.sa-santiago-1.oraclecloud.com/p/p1E-ju_3JMGILHrsBzAiS7GE8LkBi-lQvks-IJBlJS1k9CdIqKv_5ivuU-nJ0iG-/n/axosppplfddw/b/bucket-scl-prod-monitoreosscc-datalake-001/o/"

HUAWEI_COUNTER_INVENTORY_S3 = 'Huawei/Inventory/HUAWEI_COUNTERS_INVENTORY.csv'

SAMPLING = 15
HUAWEI_COUNTERS_PATH = 'Huawei/Counters_%smin'%SAMPLING

NAMESPACE_K8S = 'airflow2'

CLUSTER_DASK_IP = 'dask-cluster-airflow-scheduler.dask-cluster:8786'

REDIS_URL = 'redis-huawei-master.airflow2'
REDIS_EXPIRE = 60*60*24*2  #2 dias

VERSION = 5

def upload_parquet_s3(data:pd.DataFrame, filename:str, s3_api):
    _parquet_buffer = BytesIO()
    data.to_parquet(_parquet_buffer)
    
    _res = s3_api.Object(BUCKET, filename).put(Body=_parquet_buffer.getvalue())
    
    return list(_res.items())[0][1]['HTTPStatusCode'] == 200



def read_parquet_s3(path:str, s3_api):
    obj_buffer = s3_api.Object(BUCKET, path)

    with BytesIO(obj_buffer.get()['Body'].read()) as buffer:
        _df = pd.read_parquet(buffer)
    
    return _df



@task(
    executor_config={'LocalExecutor': {}},
)
def check_counter_file(ds=None, ti=None):


    s3_api = boto3.resource('s3',
        aws_access_key_id = ACCESS_KEY,
        aws_secret_access_key = SECRET_KEY,
        region_name = REGION, 
        endpoint_url = ENDPOINT 
    )
    
    _s3_api_buffer = s3_api.Object(BUCKET, HUAWEI_COUNTER_INVENTORY_S3)
    
    with BytesIO(_s3_api_buffer.get()['Body'].read()) as buffer:
        _readed = pd.read_csv(buffer, sep=';')
        
    _readed = _readed[pd.notnull(_readed['Id'])]
    _readed.Id = pd.to_numeric(_readed.Id, downcast='integer', errors='raise').astype(int)
    _tables_ids = _readed.table_id.unique().tolist()
    
    _dict_inventory ={x.Id:x.Counter for k,x in _readed.iterrows()}
    
    ti.xcom_push(key='dict_inventory', value=_dict_inventory)
    ti.xcom_push(key='tables_ids', value=_tables_ids)
       
    return True


@task(
    executor_config={'LocalExecutor': {}},
)
def get_dates(yesterday_ds = None, ds=None, ti=None, data_interval_start=None,  **kwargs):
    #2024-01-07
    
    print("Yesteraday Date in RAW version: %s "%data_interval_start)
    _date_oper = data_interval_start-timedelta(hours=HOURS_DELAY)
    _hour_oper = _date_oper.hour
    
    
    _date_prefix_by_day = "%s%s%s"%(
        _date_oper.year,
        str(_date_oper.month).zfill(2),
        str(_date_oper.day).zfill(2)
    )
    
    _date_prefix_by_hour = "%s%s%s%s"%(
        _date_oper.year,
        str(_date_oper.month).zfill(2),
        str(_date_oper.day).zfill(2),
        str(_date_oper.hour).zfill(2)
        )
    
    _path_out_dir = "%s/%s/%s/%s"%(
        HUAWEI_COUNTERS_PATH,
        _date_oper.year,
        str(_date_oper.month).zfill(2),
        str(_date_oper.day).zfill(2)
        )
    
      
    ti.xcom_push(key='hour_oper', value=_hour_oper)
    ti.xcom_push(key='date_prefix_by_hour', value=_date_prefix_by_hour)
    ti.xcom_push(key='date_prefix_by_day', value=_date_prefix_by_day)
    ti.xcom_push(key='path_out_dir', value=_path_out_dir)
    

    return True

@task(
    executor_config={'LocalExecutor': {}},
)
def check_redis(**kwargs):

   _redis_api = Redis(REDIS_URL, socket_connect_timeout=1) # short timeout for the test
   print(REDIS_URL)
   if not _redis_api.ping():
       raise AirflowFailException("PING REDIS TEST: NOK")
       
   print("Redis Server %s is OK"%REDIS_URL)
   return True
 
@task(
    executor_config={'LocalExecutor': {}}
)
def get_list_files(remote_path, conn_id, ti=None, **kwargs):
   
    
    _date_prefix_by_day  = ti.xcom_pull(task_ids='get_dates', key='date_prefix_by_day')
    print("Filter Dates by day:%s "%_date_prefix_by_day)
    
    _date_prefix_by_hour  = ti.xcom_pull(task_ids='get_dates', key='date_prefix_by_hour')
    print("Filter Dates by hour:%s "%_date_prefix_by_hour)
    
    _tables_ids  = ti.xcom_pull(task_ids='check_counter_file', key='tables_ids')
    print("Table Ids:%s "%_tables_ids)
       
    ## SAMPLING FILES DE 60 min
    #_regex = r'.*_%s_%s[\d]{4}_%s[\d]{4}\.csv\.gz$'%(SAMPLING,_date_prefix_by_day,_date_prefix_by_day)
    _regex = r'.*HOST\d+_pmresult_\d+_%s_\d{12}_\d{12}\.csv\.gz'%(SAMPLING)
    
 
    _remote_path = remote_path+"/pmexport_"+_date_prefix_by_day
    print("Regex prefix: %s "%_regex)
    print("Remote Path: %s"%_remote_path)

    conn = SFTPHook(ftp_conn_id=conn_id)
    print('Get list Files SFTP server: .............')
    _list_of_files_0 = conn.list_directory(_remote_path)
    

    _list_of_files_1 = [
        _remote_path+"/"+_dir0 for _dir0 in _list_of_files_0 if (re.match(_regex,_dir0)) and (int(_dir0.split('_')[2])  in _tables_ids)  
     ]
    
    
    ti.xcom_push(key='list_files', value=_list_of_files_1)
    conn.close_conn()

    return True


@task(
    queue="kubernetes",
    executor_config={
                "KubernetesExecutor": {"namespace": NAMESPACE_K8S}
    },
    pool="HUAWEI_CM_POOL_HOURS"
)
def download_files(task_id_nm,  conn_id, ti=None, **kwargs):
    import time
    print("Task: %s"%task_id_nm)
    
    s3_api = boto3.resource('s3',
        aws_access_key_id = ACCESS_KEY,
        aws_secret_access_key = SECRET_KEY,
        region_name = REGION, 
        endpoint_url = ENDPOINT 
    )
    
    redis_cli = Redis(host=REDIS_URL, port=6379, db=0)
    
    _dict_inventory = ti.xcom_pull(task_ids='check_counter_file', key='dict_inventory') 
    _list_files_paths = ti.xcom_pull(task_ids=task_id_nm, key='list_files')
    _date_prefix_by_hour  = ti.xcom_pull(task_ids='get_dates', key='date_prefix_by_hour')
    _date_prefix_by_day  = ti.xcom_pull(task_ids='get_dates', key='date_prefix_by_day')
    
    
    _path_out_dir = ti.xcom_pull(task_ids='get_dates', key='path_out_dir')
    _path_out_dir_tmp = _path_out_dir + "/tmp"
    
    print(_list_files_paths)
    print(_date_prefix_by_hour)
    print(_path_out_dir)
    
    _list_files_temp_local = {
        x.split('/')[-1]: tempfile.NamedTemporaryFile(prefix='huawei_tmp_').name
        for x in _list_files_paths
    }
    

    _len_list_files_paths = len(_list_files_paths)
    print("Files in Server: %s"%_len_list_files_paths)
    conn = SFTPHook(ftp_conn_id=conn_id)

    downloaded_files = 0
    downloaded_list = []
    for i,_path_remote in enumerate(_list_files_paths):
        _path_local_tmp = _list_files_temp_local[_path_remote.split('/')[-1]]
        _s3_file = "%s/%s"%(_path_out_dir,_path_remote.split('/')[-1])
        
        if redis_cli.exists(_s3_file):
            continue

        
        if i%10 == 0:
            _prct = 100*(i/_len_list_files_paths)
            print("Downloading file: %s  to  %s:        %0.1f%%    to   S3:%s"%( _path_remote, _path_local_tmp, _prct, _s3_file))

        # Downlaoding File, Upload S3 and set Redis
        
        conn.retrieve_file(_path_remote, _path_local_tmp)
        s3_api.meta.client.upload_file(_path_local_tmp, BUCKET, _s3_file)
        redis_cli.set(_s3_file, 1, ex=REDIS_EXPIRE)
        downloaded_files += 1
        downloaded_list.append(_path_remote)
        
    conn.close_conn()
    print("Files Downloaded: %s"%downloaded_files)
    ti.xcom_push(key='downloaded_list', value=downloaded_list)
  
    return True
 

@task(
    executor_config={'LocalExecutor': {}},
    pool="DASK_AIRFLOW"
)
def create_report_dairy(ti=None, **kwargs):
   
    import numpy as np
    from random import randint
    
    _hour_oper = ti.xcom_pull(task_ids='get_dates', key='hour_oper') 
    _path_out_dir = ti.xcom_pull(task_ids='get_dates', key='path_out_dir') 
    _date_prefix_by_day = ti.xcom_pull(task_ids='get_dates', key='date_prefix_by_day') 

    
    
    
    if _hour_oper < 23:
        print('Wating for end of the day')
        raise AirflowSkipException('Wating for final of the day')

    
    
    REPORT_COLS = [
            'SITE',
            'CELL',
            'TECH',
            'PERIOD_START_TIME'
        ]

    COLS_RAWDATA = [
        {'table_id': 1526726664, 'Id':1526729024, 'Counter':'L.Thrp.Time.DL.RmvLastTTI.QCI.9'},   #1526726664
        {'table_id': 1526726664, 'Id':1526729023, 'Counter':'L.Thrp.Time.DL.RmvLastTTI.QCI.8'},   #1526726664
        {'table_id': 1526726664, 'Id':1526729022, 'Counter':'L.Thrp.Time.DL.RmvLastTTI.QCI.7'},   #1526726664
        {'table_id': 1526726664, 'Id':1526729021, 'Counter':'L.Thrp.Time.DL.RmvLastTTI.QCI.6'},   #1526726664


        
        {'table_id': 1526726664, 'Id':1526729014, 'Counter':'L.Thrp.bits.DL.LastTTI.QCI.9'},  #1526726664
        {'table_id': 1526726664, 'Id':1526729013, 'Counter':'L.Thrp.bits.DL.LastTTI.QCI.8'},  #1526726664
        {'table_id': 1526726664, 'Id':1526729012, 'Counter':'L.Thrp.bits.DL.LastTTI.QCI.7'},  #1526726664
        {'table_id': 1526726664, 'Id':1526729011, 'Counter':'L.Thrp.bits.DL.LastTTI.QCI.6'},  #1526726664


        
        {'table_id': 1526726664, 'Id':1526726827, 'Counter':'L.Thrp.bits.DL.QCI.9'},   #1526726664
        {'table_id': 1526726664, 'Id':1526726824, 'Counter':'L.Thrp.bits.DL.QCI.8'},   #1526726664
        {'table_id': 1526726664, 'Id':1526726821, 'Counter':'L.Thrp.bits.DL.QCI.7'},   #1526726664
        {'table_id': 1526726664, 'Id':1526726818, 'Counter':'L.Thrp.bits.DL.QCI.6'},   #1526726664

        {'table_id': 1526726664, 'Id':1526728261, 'Counter':'L.Thrp.bits.DL'},   #1526726664
        {'table_id': 1526726664, 'Id':1526727064, 'Counter':'L.Thrp.bits.DL.Max'},   #1526726664
        {'table_id': 1526726664, 'Id':1526729005, 'Counter':'L.Thrp.bits.DL.LastTTI'},   #1526726664
        {'table_id': 1526726664, 'Id':1526729015, 'Counter':'L.Thrp.Time.DL.RmvLastTTI'},   #1526726664


        
        {'table_id': 1526726705, 'Id':1526727379, 'Counter':'L.Traffic.User.Max'},  #1526726705
        {'table_id': 1526726705, 'Id':1526727378, 'Counter':'L.Traffic.User.Avg'},  #1526726705
        
        ##### 5G  ####################
        {'table_id': 1911816247, 'Id':1911816643, 'Counter':'N.ThpVol.DL'},  #
        {'table_id': 1911816247, 'Id':1911816694, 'Counter':'N.ThpVol.DL.Cell'},  #
        {'table_id': 1911816243, 'Id':1911816772, 'Counter':'N.User.RRCConn.Max'},  #
        

        #'L.Thrp.bits.DL':,
        #'L.Traffic.User.Avg':,
        #'VS.HSDPA.DataOutput.Traffic':,
        #'VS.HSDPA.All.ScheduledNum':,
        #'VS.SRNCIubBytesHSDPA.Tx':,
        #'VS.SRNCIubBytesHSDSCHSig.Tx':,
        #'VS.HSDPA.UE.Mean.Cell':
    ]
    
    def proc_csv(path:str):


        REPORT_COLS = [
            'SITE',
            'CELL',
            'TECH',
            'PERIOD_START_TIME'
        ]

        def proc_idx(text:str):
            
            if re.match(r'\d+([isp])?_\d\d\d_[a-z0-9]+_[a-z0-9]+-[\d]+', text):
                _params = text.split('_')
                _site = "%s_%s"%(_params[0], _params[1])
                _tech = _params[2]
                _cell = text
                
                return [_site,_cell,_tech]
        
            if re.match(r'l\d+va\d+_\d+', text):
                _params = text.split('_')
                _site = "%s"%(_params[0])
                _tech = '4G'
                _cell = text
                
                return [_site,_cell,_tech]
        
            if re.match(r'm[a-zA-Z0-9]+_\d+_[a-zA-Z0-9]+_[a-zA-Z0-9]+-[\d]+', text):
                _params = text.split('_')
                _site = "%s_%s"%(_params[0], _params[1])
                _tech = _params[2]
                _cell = text
                
                return [_site,_cell,_tech]
            
            if re.match(r'(toy|sae)_huawei[a-z0-9]+_[a-z0-9]+-[\d]+', text):
                _params = text.split('_')
                _site = "%s_%s"%(_params[0], _params[1])
                _tech = '4G'
                _cell = text
                
                return _site,_cell,_tech
                
            _rand = randint(1000,10000)
            return ['unknown_%s'%_rand,'unknown_%s'%_rand,'unknown_%s'%_rand]

        #print("##################DW %s"%path)
        #print("##################DW %s"%(URL_PRESHARED + path) )
        try:
            _data = pd.read_csv(URL_PRESHARED + path, sep=',', compression='gzip', low_memory=False)
        except EOFError:
            print("Error reading file: %s"%path)
            return pd.DataFrame()
            
        _data = _data[pd.notnull(_data['Object Name'])]
        
        _data[['Cell_ID_1','ENODEID']] = _data['Object Name'].str.lower().str.extract(r'.*cell\ name=([0-9a-z_-]+_[345]g_[0-9a-z_-]+).*enodeb\ +id=(\d+).*')
        _data['Cell_ID_2'] = _data['Object Name'].str.lower().str.extract(r'.*label=([0-9a-z_-]+_3g_[0-9a-z_-]+).*')[0]
        _data[['Cell_ID_1','Cell_ID_2']] = _data[['Cell_ID_1','Cell_ID_2']].fillna('')
        _data['Cell_ID'] = _data['Cell_ID_1'] + _data['Cell_ID_2']
        
        _data = _data.drop(['Cell_ID_1','Cell_ID_2','Reliability','Granularity Period'], axis=1)
        
        _numeric_cols = _data.columns[_data.columns.str.contains(r'\d+')].tolist()
        if len(_numeric_cols) == 0:
            return pd.DataFrame()
        
        _data[_numeric_cols] = _data[_numeric_cols].apply(lambda x: pd.to_numeric(x, errors='coerce'))
        _data = _data.set_index('Cell_ID').join(
            pd.DataFrame([{'SITE':x[0],'CELL':x[1], 'TECH':x[2]} for x in _data.Cell_ID.str.lower().apply(proc_idx)]).set_index('CELL')
        )
        
        _data.reset_index(inplace=True)
        _data = _data.rename(columns={'Result Time':'PERIOD_START_TIME', 'Cell_ID':'CELL'})
        _data['PERIOD_START_TIME'] = pd.to_datetime(_data['PERIOD_START_TIME'], format='%Y-%m-%d %H:%M')
        _data = _data[pd.notnull(_data['PERIOD_START_TIME'])] 

        _data = _data[REPORT_COLS + ['ENODEID'] +  _numeric_cols]
        _data = _data.set_index(['SITE','CELL','TECH','PERIOD_START_TIME','ENODEID'])

        _data = _data[_numeric_cols]
        _data.columns = [int(x) for x in _data.columns]

        _data = _data.groupby(level=['SITE','CELL','TECH','PERIOD_START_TIME','ENODEID'], as_index=True).max()
        
        return _data
    
    
    def proc_cell_4g(data:pd.DataFrame, remove_outliners=False):

        def remove_outliners_from_serie(serie:pd.Series):

            _serie = serie.infer_objects(copy=False)
            _q_hi  = _serie.quantile(0.90)
            
            _serie[(_serie > _q_hi)] = pd.NA
            
            return _serie

        _data = data.copy()

        _data['VOL'] = (_data['L.Thrp.bits.DL'] ) / 1000000   #Mbits
        _data['THRPUT'] = _data['VOL'] / (SAMPLING*60)   #Mbits/s  # 15 minutes
        _data['CCUSERS'] = _data['L.Traffic.User.Max']  # #concurrent users
        _data['USER_THRPUT'] = _data.apply(lambda x: x['THRPUT'] / x['CCUSERS'] if x['CCUSERS'] > 0 else pd.NA, axis=1) #Mbits/s
        _data  = _data[['VOL','THRPUT','CCUSERS','USER_THRPUT']]

        if remove_outliners:
            _data['THRPUT' ]= _data.groupby(level=['SITE','CELL'], as_index=False)['THRPUT'].apply(remove_outliners_from_serie).droplevel(0)

        _data = _data.reset_index()[['SITE','CELL','TECH','ENODEID','PERIOD_START_TIME','VOL', 'THRPUT', 'CCUSERS', 'USER_THRPUT']]

        return _data
        
    s3_api = boto3.resource('s3',
        aws_access_key_id = ACCESS_KEY,
        aws_secret_access_key = SECRET_KEY,
        region_name = REGION, 
        endpoint_url = ENDPOINT 
    )
    
    TABLES_NAMES = pd.DataFrame(COLS_RAWDATA).table_id.unique()
    COLS_NAMES = {x['Id']:x['Counter'] for x in COLS_RAWDATA}
    ID_NAMES = pd.DataFrame(COLS_RAWDATA).Id.unique()
    
         
    
    FILES_ALL = [x.key for x in s3_api.Bucket(BUCKET).objects.filter(Prefix=_path_out_dir)]

    FILES = []
    ### ESTE FILTRO!
    for table in TABLES_NAMES:
        FILES = FILES + [x for x in FILES_ALL if re.match(r'.*_%s_%s_.*\.csv\.gz$'%(table,SAMPLING),x)]


    FILES_DF = pd.DataFrame(FILES, columns=['path'])
    FILES_DF['start_date'] = FILES_DF.path.str.split('_').apply(lambda x: pd.to_datetime(x[5], format='%Y%m%d%H%M'))
    FILES_DF['end_date'] = FILES_DF.path.str.split('_').apply(lambda x: pd.to_datetime(x[6].split('.')[0], format='%Y%m%d%H%M'))
    FILES_DF['table'] = FILES_DF.path.str.split('_').apply(lambda x: x[3] )
    FILES_DF['host'] = FILES_DF.path.str.split('_').apply(lambda x: x[1].split('/')[-1] )
    
    print(FILES_DF.sample(10))
    print("######################################")
    print("Retreiving files from %s"%_path_out_dir)
    
    _path_file_s3 = _path_out_dir.replace('Counters_%smin'%SAMPLING,'Reports_%smin'%SAMPLING)
    _path_file_s3 = '/'.join( _path_file_s3.split('/')[:3])
    print("Preparing report to %s"%_path_file_s3)
    
   
    print('Reading and Concat Files')  
    DATA_COUNTERS = pd.DataFrame()        
    with Client(CLUSTER_DASK_IP) as DASK_CLIENT:

        _to_joins = []
        for k,v in FILES_DF.groupby('table'):
            futures = DASK_CLIENT.map(proc_csv, v.path)
            _to_append = DASK_CLIENT.submit(pd.concat, futures).result()
            _to_append = _to_append[ [x for x in _to_append.columns if x in ID_NAMES] ]
            _to_joins.append(_to_append)
       
    print('Joining Files')  
    DATA_COUNTERS = pd.DataFrame()
    for _to_join in _to_joins:
        DATA_COUNTERS = _to_join if DATA_COUNTERS.empty else DATA_COUNTERS.join(_to_join)

    DATA_COUNTERS = DATA_COUNTERS[COLS_NAMES.keys()]
    DATA_COUNTERS.columns = [COLS_NAMES[x] for x in COLS_NAMES.keys()]   


    #PROCESSING 4G
    print('Processig 4G Files')  
    #DATA_COUNTERS_4G = DATA_COUNTERS.loc[DATA_COUNTERS.index.get_level_values('TECH').str.lower() == '4g',:].copy()
    DATA_COUNTERS_4G = DATA_COUNTERS.copy()
    DATA_COUNTERS_4G = DATA_COUNTERS_4G.groupby(level='CELL', as_index=False).apply(proc_cell_4g)
         
    _report_output = "%s/REPORT_HUAWEI_%s.parquet"%(
        _path_file_s3,
        _date_prefix_by_day
    )
    print("Guardando Reporte en: %s"%_report_output)
    upload_parquet_s3(DATA_COUNTERS_4G,_report_output, s3_api)
    
    ti.xcom_push(key='report_output', value=_report_output)
    return True
    
with DAG(
    dag_id='dw_huawei_counters_15min_3',
    schedule_interval= "@hourly",
    default_args={
        "depends_on_past": False,
        'owner': 'Vmasuero',
        "retries": 3,
        'max_active_runs': 1,
        "retry_delay": timedelta(minutes=5)
    },
    start_date=pendulum.datetime( 2024, 8, 3, tz='America/Santiago'),
    catchup=False,
    tags=['development', 'huawei']
) as dag:

'''
    with TaskGroup(group_id='dw_tasks') as dw_tasks:

        for i,server_conn in enumerate(PM_HUAWEI_SERVERS[:]):
            _task_id_get_list_files = ('dw_tasks.get_list_files__'+str(i)).replace('__0','')

            chain(
                get_list_files(conn_id=server_conn, remote_path=REMOTE_PATH),
                download_files(conn_id=server_conn, task_id_nm=_task_id_get_list_files)
            )

   '''           
    chain(
        get_dates(),
        #check_counter_file(),
        #check_redis(),
        #dw_tasks,
        #create_report_dairy()
    )