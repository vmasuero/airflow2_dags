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


S3_PATH = f"NETWORK_COUNTERS/OYM_v3"
DIARY_REPORTS_PATH =  "NETWORK_COUNTERS/REPORT_DIARY_v3"
WEEKLY_REPORTS_PATH =  "NETWORK_COUNTERS/REPORT_WEEKLY_v3"

HEADERS_PATH = 'NETWORK_COUNTERS/HEADERS'
DIARY_REPORT_DIR = f'NETWORK_COUNTERS/REPORT_DIARY_v3/{int(datetime.now().strftime('%Y'))}'


SAMPLING = '5min'

REMOTE_COLS = [
    'devname',
    'ifName',
    'ifAlias',
    'time',
    'input',
    'output',
    'ifspeed',
    'ifadmin',
    'iftype',
    'devif'
]

HEADERS_COLS = [
        'Empresa', 
        'Instancia 0', 
        'Instancia 1', 
        'Instancia 2',
        'Localidad A', 
        'Extremo A', 
        'Pta A', 
        'Descripcion',
        'Localidad B',
        'Extremo B', 
        #'Pta B',
        'Capacidad',
        'Devif'
]




def read_parquet_s3( s3_api, path:str, bucket:str, cols=[]):
    obj_buffer = s3_api.Object(bucket, path)

    with BytesIO(obj_buffer.get()['Body'].read()) as buffer_fd:
        
        if len(cols) > 0:
            _df = pd.read_parquet(buffer_fd, columns=cols)
        else:
            _df = pd.read_parquet(buffer_fd)

    return _df 
    
def read_csv_from_s3( s3_api, bucket, path:str):
    obj_buffer = s3_api.Object(bucket, path)
    
    with BytesIO(obj_buffer.get()['Body'].read()) as buffer:
        _df = pd.read_csv(buffer, sep=';')
        
    
    return _df    


def read_parquet_from_s3( s3_api, bucket, path:str, cols=None):
    obj_buffer = s3_api.Object(bucket, path)
    
    with BytesIO(obj_buffer.get()['Body'].read()) as buffer:
        _df = pd.read_parquet(buffer, columns=cols)
        
    
    
    return _df   

def upload_parquet_s3(s3_api, bucket, data:pd.DataFrame, filename:str):

    _parquet_buffer = BytesIO()
    data.to_parquet(_parquet_buffer)
    
    _res = s3_api.Object(bucket, filename).put(Body=_parquet_buffer.getvalue())
    
    return list(_res.items())[0][1]['HTTPStatusCode'] == 200

def upload_excel_s3(s3_api, bucket, data:pd.DataFrame, filename:str):

    _buffer = BytesIO()
    data.to_excel(_buffer, index=False)
    
    _res = s3_api.Object(bucket, filename).put(Body=_buffer.getvalue())
    
    return list(_res.items())[0][1]['HTTPStatusCode'] == 200


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
    _reports_dir = "%s/%s"%(DIARY_REPORTS_PATH,_year)
    _reports_dir_weekly = "%s/%s"%(DIARY_REPORTS_PATH,_year)

    
    _remote_file = f"{PREFIX}/{ds_nodash}_ClaroVtr_Traffic_v3.parquet"
    
    
    #traffic/20250812_ClaroVtr_Traffic_v3.parquet
    
    _bucket = _s3_api.Bucket(OCI_BUCKET)
    _header_files = [obj.key for obj in _bucket.objects.filter(Prefix=HEADERS_PATH)]
    _header_file = get_last_version_file(_header_files)
    _header_file_prefix = _header_file.split('/')[-1].split('.')[0]
    
    _remote_file_oci = f"{_output_dir}/{_remote_file.split('/')[-1]}"
    
    
    _report_file_xls = f'{DIARY_REPORT_DIR}/{ds_nodash[:4]}-{ds_nodash[4:6]}-{ds_nodash[6:8]}_{_header_file_prefix}.xls'
    _report_file_parquet = f'{DIARY_REPORT_DIR}/{ds_nodash[:4]}-{ds_nodash[4:6]}-{ds_nodash[6:8]}_{_header_file_prefix}.parquet'

    
    
    ti.xcom_push(key='output_dir', value=_output_dir)
    ti.xcom_push(key='remote_file', value=_remote_file)
    ti.xcom_push(key='remote_file_oci', value=_remote_file_oci)
    ti.xcom_push(key='header_file', value=_header_file)
    ti.xcom_push(key='header_file_prefix', value=_header_file_prefix)
    ti.xcom_push(key='report_file_xls', value=_report_file_xls)
    ti.xcom_push(key='report_file_parquet', value=_report_file_parquet)
    ti.xcom_push(key='reports_dir', value=_reports_dir)
    ti.xcom_push(key='reports_dir_weekly', value=_reports_dir_weekly)
    

    return True


@task(
    executor_config={'LocalExecutor': {}},
    pool='SERIAL'
    
)
def dowload_upload_raw(yesterday_ds = None, ds=None, ti=None, ds_nodash=None,  **kwargs):

    _remote_file = ti.xcom_pull(task_ids='initialization', key='remote_file') 
    _remote_file_oci = ti.xcom_pull(task_ids='initialization', key='remote_file_oci') 

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
        _bucket_oci.upload_file(tmp_path, _remote_file_oci, ExtraArgs={"ContentType": "application/x-parquet"})
    finally:
        print(f"Deleting temp file: {tmp_path}")
        os.remove(tmp_path) 
    
    return True
    
  
@task(
    executor_config={'LocalExecutor': {}},
    pool='SERIAL'
    
)
def create_daily_report(yesterday_ds = None, ds=None, ti=None, ds_nodash=None,  **kwargs):

    def proc_header_file(data:pd.DataFrame):
        _headers = data.copy()
        _headers = _headers[pd.notnull(_headers['Extremo A'])]
        
        _headers = _headers[HEADERS_COLS]
        _headers['Empresa'] = _headers['Empresa'].astype(str)
        _headers['Instancia 0'] = _headers['Instancia 0'].astype(str)
        _headers['Instancia 1'] = _headers['Instancia 1'].astype(str)
        _headers['Instancia 2'] = _headers['Instancia 2'].astype(str) 
        _headers['Localidad A'] = _headers['Localidad A'].astype(str) 
        _headers['Extremo A'] = _headers['Extremo A'].str.lower().str.strip()
        _headers['Extremo B'] = _headers['Extremo B'].str.lower().fillna('unknow').str.strip()
        _headers['Pta A'] = _headers['Pta A'].str.lower().str.replace("'","").str.strip()
        _headers['Localidad B'] = _headers['Localidad B'].astype(str).fillna('unknow').str.strip()
        
        _headers.loc[ _headers['Localidad A'] == 'nan', 'Localidad A'] = 'unknow'
        _headers.loc[ _headers['Localidad B'] == 'nan', 'Localidad B'] = 'unknow'
        
        _headers['Descripcion'] = _headers['Descripcion'].astype(str)
        _headers['Capacidad'] = _headers['Capacidad'].astype(float)
        
        _headers[_headers.select_dtypes('object').columns] = _headers[_headers.select_dtypes('object').columns].apply(lambda x: x.str.strip())


        _headers = _headers[HEADERS_COLS]
        _headers = _headers.set_index('Devif')


        return _headers    

    def proc_data_file(data:pd.DataFrame):
        MEGA = 1000000

        
        def remove_outliners(g, cols=("input", "output"), factor=1.5):

            for col in cols:
                Q1 = g[col].quantile(0.25)
                Q3 = g[col].quantile(0.75)
                IQR = Q3 - Q1
                #lower = Q1 - factor * IQR
                upper = Q3 + factor * IQR
        
                mean_val = g[col].mean()
        
                #mask = (g[col] < lower) | (g[col] > upper)
                mask = g[col] > upper

                g.loc[mask, col] = int(mean_val)
                
            return g
        
        print('Filtering')
        data =  data[data.devname != 'nan']
        data =  data[data.ifAlias != 'nan']
        data =  data[data.ifName != 'nan']
        data = data[data.ifadmin == 1]
        data = data.drop(['ifadmin'], axis=1)
        data.devname = data.devname.apply(lambda x:  x.split('.')[0] )
        data = data.set_index('devif')

        print('Converting Dates')
        data['time'] = pd.to_datetime(data['time'], format="%Y-%m-%d %H:%M:%S")
        data['input'] = data['input'] / MEGA
        data['output'] = data['input'] / MEGA
        
        print('Checking active links')
        active_links = data[['input','output']].copy().max(axis=1).groupby(level=0).mean()
        active_links = active_links[active_links > 5].index
        data = data.loc[ data.index.isin(active_links) ]
        
        print('Removing outliners')
        data = data.groupby(level=0, group_keys=False).apply(remove_outliners)

        

        return data

    def create_summaries(data):
        SUMMARIES = []
        _data_report = data.copy()
        
        print('Adding Summaries')
        _data_report_0 = _data_report.copy()
        _data_report_0['Total'] = 0
        SUMMARIES.append(_data_report_0)
    
        ## ########### BY Device
        _data_report_1 = _data_report.groupby([
            'Direccion',
            'Empresa',
            'Instancia 0', 
            'Instancia 1',
            'Instancia 2',
            'Localidad A',
            'Extremo A'
        ],as_index=False).sum(numeric_only=True)
        _data_report_1['Total'] = 1
        SUMMARIES.append(_data_report_1)
    
        ############ By Instancia 2
        _data_report_2 = _data_report.groupby([
            'Direccion',
            'Empresa',
            'Instancia 0', 
            'Instancia 1',
            'Instancia 2'
        ],as_index=False).sum(numeric_only=True)
        _data_report_2['Total'] = 2
        SUMMARIES.append(_data_report_2)
    
        ############ By Instancia 1
        _data_report_3 = _data_report.groupby([
            'Direccion',
            'Empresa',
            'Instancia 0', 
            'Instancia 1'
        ],as_index=False).sum(numeric_only=True)
        _data_report_3['Total'] = 3
        SUMMARIES.append(_data_report_3)
    
        ############ By Total
        _data_report_4 = _data_report.groupby([
            'Direccion',
            'Instancia 0', 
            'Instancia 1',
            'Instancia 2'
        ],as_index=False).sum(numeric_only=True)
        _data_report_4['Total'] = 4
        SUMMARIES.append(_data_report_4)
    
        ############ By Localidad
        _data_report_5 = _data_report.groupby([
            'Direccion',
            'Empresa',
            'Instancia 0', 
            'Instancia 1',
            'Instancia 2',
            'Localidad A'
        ],as_index=False).sum(numeric_only=True)
        _data_report_5['Total'] = 5
        SUMMARIES.append(_data_report_5)
    
    
        ############ Total por empresa
        _data_report_11 = _data_report.groupby([
            'Direccion',
            'Empresa',
            'Instancia 0'
        ],as_index=False).sum(numeric_only=True)
        _data_report_11['Total'] = 6
        SUMMARIES.append(_data_report_11)
    
        ############ Total 
        _data_report_12 = _data_report.groupby([
            'Direccion',
            'Instancia 0'
        ],as_index=False).sum(numeric_only=True)
        _data_report_12['Total'] = 7
        SUMMARIES.append(_data_report_12)
    
        #### Total Instancia 1 sin empresa
        _data_report_13 = _data_report.groupby([
            'Direccion',
            'Instancia 0', 
            'Instancia 1'
        ],as_index=False).sum(numeric_only=True)
        _data_report_13['Total'] = 8
        SUMMARIES.append(_data_report_13)
    
        #### Total Instancia 1 sin empresa
        _data_report_14 = _data_report.groupby([
            'Direccion',
            'Instancia 0', 
            'Instancia 1',
            'Instancia 2'
        ],as_index=False).sum(numeric_only=True)
        _data_report_14['Total'] = 9
        SUMMARIES.append(_data_report_14)
    
        ####################################################### EXTREMO B ###############################
        ############ By Extremo A y Extremo B
        _data_report_6 = _data_report[ _data_report['Extremo B'] != 'unknow'].groupby([
            'Direccion',
            'Instancia 0', 
            'Instancia 1',
            'Instancia 2',
            'Extremo A',
            'Extremo B'
        ],as_index=False).sum(numeric_only=True)
        _data_report_6['Total'] = 101
        SUMMARIES.append(_data_report_6)
    
        ############ By Extremo A y Localidad B
        _data_report_7 = _data_report[ _data_report['Localidad B'] != 'unknow'].groupby([
            'Direccion',
            'Instancia 0', 
            'Instancia 1',
            'Instancia 2',
            'Extremo A',
            'Localidad B'
        ],as_index=False).sum(numeric_only=True)
        _data_report_7['Total'] = 102
        SUMMARIES.append(_data_report_7)
    
    
        ############ By Extremo A y Extremo B, y empresa
        _data_report_8 = _data_report[ _data_report['Extremo B'] != 'unknow'].groupby([
            'Direccion',
            'Instancia 0', 
            'Instancia 1',
            'Instancia 2',
            'Extremo A',
            'Extremo B'
        ],as_index=False).sum(numeric_only=True)
        _data_report_8['Total'] = 103
        SUMMARIES.append(_data_report_8)
    
        ############ By Extremo A y Localidad B, y empresa
        _data_report_9 = _data_report[ _data_report['Localidad B'] != 'unknow'].groupby([
            'Direccion',
            'Empresa',
            'Instancia 0', 
            'Instancia 1',
            'Instancia 2',
            'Extremo A',
            'Localidad B'
        ],as_index=False).sum(numeric_only=True)
        _data_report_9['Total'] = 104
        SUMMARIES.append(_data_report_9)
    
        ############ By Extremo A y Localidad B, y empresa
        _data_report_10 = _data_report[ _data_report['Localidad B'] != 'unknow'].groupby([
            'Direccion',
            'Empresa',
            'Instancia 0', 
            'Instancia 1',
            'Instancia 2',
            'Localidad B'
        ],as_index=False).sum(numeric_only=True)
        _data_report_10['Total'] = 105
        SUMMARIES.append(_data_report_10)
    
        ############ By Extremo A y Extremo B
        _data_report_11 = _data_report[ _data_report['Extremo B'] != 'unknow'].groupby([
            'Direccion',
            'Instancia 0', 
            'Instancia 1',
            'Instancia 2',
            'Extremo A',
            'Extremo B'
        ],as_index=False).sum(numeric_only=True)
        _data_report_11['Total'] = 106
        SUMMARIES.append(_data_report_11)
    
        ############ Extremo B
        _data_report_12 = _data_report[ _data_report['Extremo B'] != 'unknow'].groupby([
            'Direccion',
            'Instancia 0', 
            'Instancia 1',
            'Instancia 2',
            'Extremo B'
        ],as_index=False).sum(numeric_only=True)
        _data_report_12['Total'] = 107
        SUMMARIES.append(_data_report_12)
    
        _data_report_13 = _data_report[ _data_report['Extremo B'] != 'unknow'].groupby([
            'Direccion',
            'Empresa',
            'Instancia 0', 
            'Instancia 1',
            'Instancia 2',
            'Extremo B'
        ],as_index=False).sum(numeric_only=True)
        _data_report_13['Total'] = 108
        SUMMARIES.append(_data_report_13)
    
    
        _data_all_summaries = pd.concat(SUMMARIES, axis=0, ignore_index=True)
        _data_all_summaries['BW [Mbps]'] = _data_all_summaries[TIME_COLUMNS].max(axis=1)
        _data_all_summaries['Fecha'] = pd.Series([x.strftime('%Y-%m-%d') for x in TIME_COLUMNS]).mode().values[0]
        
        _data_all_summaries = _data_all_summaries[[
                'Total',
                'Empresa', 
                'Instancia 0',
                'Instancia 1', 
                'Instancia 2', 
                'Localidad A', 
                'Extremo A',
                'Pta A', 
                'Descripcion',
                'Fecha',
                'Localidad B',
                'Extremo B',
                'Direccion',
                'Capacidad',
                'BW [Mbps]'
        ]]
    
        _data_all_summaries = _data_all_summaries.sort_values(by=[
            'Empresa', 
            'Instancia 0',
            'Instancia 1', 
            'Instancia 2', 
            'Localidad A', 
            'Extremo A', 
            'Total'
        ])
    
        return _data_all_summaries

    _remote_file = ti.xcom_pull(task_ids='initialization', key='remote_file') 
    _remote_file_oci = ti.xcom_pull(task_ids='initialization', key='remote_file_oci') 
    _header_file = ti.xcom_pull(task_ids='initialization', key='header_file')
    
    _report_file_parquet = ti.xcom_pull(task_ids='initialization', key='report_file_parquet')
    _report_file_xls = ti.xcom_pull(task_ids='initialization', key='report_file_xls')


    _s3_api_oci = boto3.resource(
        's3',
        aws_access_key_id = OCI_ACCESS_KEY,
        aws_secret_access_key = OCI_SECRET_KEY,
        region_name = OCI_REGION, 
        endpoint_url = OCI_ENDPOINT
    )
    
    print(f"Reading file {_remote_file_oci}")
    _traffic = read_parquet_from_s3(_s3_api_oci, OCI_BUCKET, _remote_file_oci, cols=REMOTE_COLS)
    print(f"Reading file {_header_file}")
    _header = read_csv_from_s3(_s3_api_oci, OCI_BUCKET, _header_file)

    _header = proc_header_file(_header)
    _traffic = _traffic[_traffic.devif.isin(_header.index)]
    _traffic = proc_data_file(_traffic)

    _traffic = pd.pivot_table(
        _traffic, 
        values=['input','output'], 
        index=['devif'], 
        columns=['time'], 
        aggfunc="max"
    ).stack(0, future_stack=True)

    TIME_COLUMNS = _traffic.columns

    _traffic = _traffic.reset_index()
    _traffic = _traffic.rename(columns={'level_1':'Direccion'})
    _traffic.Direccion = _traffic.Direccion.str.replace('input','IN')
    _traffic.Direccion = _traffic.Direccion.str.replace('output','OUT')
    _traffic = _traffic.set_index('devif')
    _traffic = _header.join(_traffic)
   
    print(f"Saving parquet file: {_report_file_parquet}")
    upload_parquet_s3(_s3_api_oci, OCI_BUCKET, _traffic, _report_file_parquet)
    
    
    print('Create Summaries')
    _traffic = create_summaries(_traffic)
    
    print(f"Saving excel file: {_report_file_xls}")
    upload_excel_s3(_s3_api_oci, OCI_BUCKET, _traffic, _report_file_xls)
    
    return True
    
   
@task(
    executor_config={'LocalExecutor': {}}
)
def create_report_weekly(ti=None, data_interval_start=None, **kwargs):

    def create_summaries(data):
            SUMMARIES = []
            _data_report = data.copy()
            _category_cols = _data_report.loc[:, pd.to_datetime(_data_report.columns, format="%Y-%m-%d %H:%M:%S", errors='coerce').isna()].columns
            _datetime_cols = _data_report.loc[:, pd.to_datetime(_data_report.columns, format="%Y-%m-%d %H:%M:%S", errors='coerce').notna()].columns
            
            
            print('Adding Summaries')
            _data_report_0 = _data_report.copy()
            _data_report_0['Total'] = 0
            SUMMARIES.append(_data_report_0)
        
            ## ########### BY Device
            _data_report_1 = _data_report.groupby([
                'Direccion',
                'Empresa',
                'Instancia 0', 
                'Instancia 1',
                'Instancia 2',
                'Localidad A',
                'Extremo A'
            ],as_index=False).sum(numeric_only=True)
            _data_report_1['Total'] = 1
            SUMMARIES.append(_data_report_1)
        
            ############ By Instancia 2
            _data_report_2 = _data_report.groupby([
                'Direccion',
                'Empresa',
                'Instancia 0', 
                'Instancia 1',
                'Instancia 2'
            ],as_index=False).sum(numeric_only=True)
            _data_report_2['Total'] = 2
            SUMMARIES.append(_data_report_2)
        
            ############ By Instancia 1
            _data_report_3 = _data_report.groupby([
                'Direccion',
                'Empresa',
                'Instancia 0', 
                'Instancia 1'
            ],as_index=False).sum(numeric_only=True)
            _data_report_3['Total'] = 3
            SUMMARIES.append(_data_report_3)
        
            ############ By Total
            _data_report_4 = _data_report.groupby([
                'Direccion',
                'Instancia 0', 
                'Instancia 1',
                'Instancia 2'
            ],as_index=False).sum(numeric_only=True)
            _data_report_4['Total'] = 4
            SUMMARIES.append(_data_report_4)
        
            ############ By Localidad
            _data_report_5 = _data_report.groupby([
                'Direccion',
                'Empresa',
                'Instancia 0', 
                'Instancia 1',
                'Instancia 2',
                'Localidad A'
            ],as_index=False).sum(numeric_only=True)
            _data_report_5['Total'] = 5
            SUMMARIES.append(_data_report_5)
        
        
            ############ Total por empresa
            _data_report_11 = _data_report.groupby([
                'Direccion',
                'Empresa',
                'Instancia 0'
            ],as_index=False).sum(numeric_only=True)
            _data_report_11['Total'] = 6
            SUMMARIES.append(_data_report_11)
        
            ############ Total 
            _data_report_12 = _data_report.groupby([
                'Direccion',
                'Instancia 0'
            ],as_index=False).sum(numeric_only=True)
            _data_report_12['Total'] = 7
            SUMMARIES.append(_data_report_12)
        
            #### Total Instancia 1 sin empresa
            _data_report_13 = _data_report.groupby([
                'Direccion',
                'Instancia 0', 
                'Instancia 1'
            ],as_index=False).sum(numeric_only=True)
            _data_report_13['Total'] = 8
            SUMMARIES.append(_data_report_13)
        
            #### Total Instancia 1 sin empresa
            _data_report_14 = _data_report.groupby([
                'Direccion',
                'Instancia 0', 
                'Instancia 1',
                'Instancia 2'
            ],as_index=False).sum(numeric_only=True)
            _data_report_14['Total'] = 9
            SUMMARIES.append(_data_report_14)
        
            ####################################################### EXTREMO B ###############################
            ############ By Extremo A y Extremo B
            _data_report_6 = _data_report[ _data_report['Extremo B'] != 'unknow'].groupby([
                'Direccion',
                'Instancia 0', 
                'Instancia 1',
                'Instancia 2',
                'Extremo A',
                'Extremo B'
            ],as_index=False).sum(numeric_only=True)
            _data_report_6['Total'] = 101
            SUMMARIES.append(_data_report_6)
        
            ############ By Extremo A y Localidad B
            _data_report_7 = _data_report[ _data_report['Localidad B'] != 'unknow'].groupby([
                'Direccion',
                'Instancia 0', 
                'Instancia 1',
                'Instancia 2',
                'Extremo A',
                'Localidad B'
            ],as_index=False).sum(numeric_only=True)
            _data_report_7['Total'] = 102
            SUMMARIES.append(_data_report_7)
        
        
            ############ By Extremo A y Extremo B, y empresa
            _data_report_8 = _data_report[ _data_report['Extremo B'] != 'unknow'].groupby([
                'Direccion',
                'Instancia 0', 
                'Instancia 1',
                'Instancia 2',
                'Extremo A',
                'Extremo B'
            ],as_index=False).sum(numeric_only=True)
            _data_report_8['Total'] = 103
            SUMMARIES.append(_data_report_8)
        
            ############ By Extremo A y Localidad B, y empresa
            _data_report_9 = _data_report[ _data_report['Localidad B'] != 'unknow'].groupby([
                'Direccion',
                'Empresa',
                'Instancia 0', 
                'Instancia 1',
                'Instancia 2',
                'Extremo A',
                'Localidad B'
            ],as_index=False).sum(numeric_only=True)
            _data_report_9['Total'] = 104
            SUMMARIES.append(_data_report_9)
        
            ############ By Extremo A y Localidad B, y empresa
            _data_report_10 = _data_report[ _data_report['Localidad B'] != 'unknow'].groupby([
                'Direccion',
                'Empresa',
                'Instancia 0', 
                'Instancia 1',
                'Instancia 2',
                'Localidad B'
            ],as_index=False).sum(numeric_only=True)
            _data_report_10['Total'] = 105
            SUMMARIES.append(_data_report_10)
        
            ############ By Extremo A y Extremo B
            _data_report_11 = _data_report[ _data_report['Extremo B'] != 'unknow'].groupby([
                'Direccion',
                'Instancia 0', 
                'Instancia 1',
                'Instancia 2',
                'Extremo A',
                'Extremo B'
            ],as_index=False).sum(numeric_only=True)
            _data_report_11['Total'] = 106
            SUMMARIES.append(_data_report_11)
        
            ############ Extremo B
            _data_report_12 = _data_report[ _data_report['Extremo B'] != 'unknow'].groupby([
                'Direccion',
                'Instancia 0', 
                'Instancia 1',
                'Instancia 2',
                'Extremo B'
            ],as_index=False).sum(numeric_only=True)
            _data_report_12['Total'] = 107
            SUMMARIES.append(_data_report_12)
        
            _data_report_13 = _data_report[ _data_report['Extremo B'] != 'unknow'].groupby([
                'Direccion',
                'Empresa',
                'Instancia 0', 
                'Instancia 1',
                'Instancia 2',
                'Extremo B'
            ],as_index=False).sum(numeric_only=True)
            _data_report_13['Total'] = 108
            SUMMARIES.append(_data_report_13)
        
        
            _data_all_summaries = pd.concat(SUMMARIES, axis=0, ignore_index=True)
            _data_all_summaries['BW [Mbps]'] = _data_all_summaries[_datetime_cols].max(axis=1)
            _data_all_summaries['Fecha'] = pd.Series([pd.to_datetime(x).strftime('%Y-%m-%d') for x in _datetime_cols]).mode().values[0]
            
            _data_all_summaries = _data_all_summaries[[
                    'Total',
                    'Empresa', 
                    'Instancia 0',
                    'Instancia 1', 
                    'Instancia 2', 
                    'Localidad A', 
                    'Extremo A',
                    'Pta A', 
                    'Descripcion',
                    'Fecha',
                    'Localidad B',
                    'Extremo B',
                    'Direccion',
                    'Capacidad',
                    'BW [Mbps]'
            ]]
        
            _data_all_summaries = _data_all_summaries.sort_values(by=[
                'Empresa', 
                'Instancia 0',
                'Instancia 1', 
                'Instancia 2', 
                'Localidad A', 
                'Extremo A', 
                'Total'
            ])
        
            return _data_all_summaries
        
    _output_dir = ti.xcom_pull(task_ids='initialization', key='output_dir') 
    _reports_dir = ti.xcom_pull(task_ids='initialization', key='reports_dir') 
    _reports_dir_weekly = ti.xcom_pull(task_ids='initialization', key='reports_dir_weekly')



    _date_current = data_interval_start
    _week = int(_date_current.strftime('%W'))
    _current_day = _date_current.strftime('%Y-%m-%d')
    _file_report = f"{_reports_dir_weekly}/{_current_day}_network_headers_W{_week}.xls"
    
    print("Week: %s"%_week)

    if _date_current.weekday() != 0:
            print('is not Monday, skip')
            raise AirflowSkipException('is not Monday, skip')

    _s3_api_oci = boto3.resource(
        's3',
        aws_access_key_id = OCI_ACCESS_KEY,
        aws_secret_access_key = OCI_SECRET_KEY,
        region_name = OCI_REGION, 
        endpoint_url = OCI_ENDPOINT
    )
    
    print(f"Buscando en directorios {_reports_dir}")
    _bucket = _s3_api_oci.Bucket(OCI_BUCKET)
    _files = [obj.key for obj in _bucket.objects.filter(Prefix=_reports_dir) if 'parquet' in obj.key]
    _files = pd.DataFrame(_files, columns=['path'])
    _files['file'] = _files.path.apply(lambda x: x.split('/')[-1])
    _files['date_f'] = _files.file.str.extract(r'(\d+-\d+-\d+)_network_headers_.*').apply(lambda x: pd.to_datetime(x, format='%Y-%m-%d'))

    _files['week'] = _files.date_f.apply(lambda x: x.isocalendar()[1]).astype(int)

    _files = _files[_files.week == _week]
    
    ti.xcom_push(key='files_report_week', value=_files.path.tolist())
    ti.xcom_push(key='reports_dir_weekly', value=_reports_dir_weekly)
    
    
    print(f'cerate report in {_file_report}')
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
    start_date=pendulum.datetime( 2025, 8, 31, tz='America/Santiago'),
    catchup=True,
    tags=['development', 'bw']
) as dag:

   
    initialization() >> dowload_upload_raw() >> create_daily_report() >> create_report_weekly()
    