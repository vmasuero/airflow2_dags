import pendulum
import pandas as pd
from datetime import datetime, timedelta
import re
import boto3 

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.decorators import dag, task
from airflow.utils.helpers import chain
from airflow.sensors.base import PokeReturnValue
from io import StringIO,BytesIO
#from dask.distributed import Client, LocalCluster, Future, as_completed, fire_and_forget


SECRET_KEY ='2DhT3mGRLmNDBOl9ZuxCLdic0jXSmfUiZ+niJrwp3cU='
ACCESS_KEY = 'd7556c3cc7c1996477a5c851b51e2f47ea4d00a6'
REGION = 'sa-santiago-1'
NAMESPACE = 'axosppplfddw'
BUCKET = 'bucket-scl-prod-monitoreosscc-datalake-001'
ENDPOINT = "https://%s.compat.objectstorage.%s.oraclecloud.com"%(NAMESPACE,REGION)


DIR_NOKIA = 'Nokia/Reports_60min'
DIR_HUAWEI = 'Huawei/Reports_60min'
DIR_OUT = 'Acceso_Movil/Reports_60min'

VERSION = "5.0"

def save_csv_s3(s3,data, bucket, path):
        
    _buffer = BytesIO()
    
    data.to_csv(_buffer, compression = "gzip", sep=";", index=False)
    _buffer.seek(0)
    
    _res = s3.Object(bucket, path).put(Body=_buffer.getvalue())
    
    return list(_res.items())[0][1]['HTTPStatusCode'] == 200

def save_excel_s3(s3, data, bucket, path):
        
    _buffer = BytesIO()
    
    data.to_excel(_buffer, sheet_name='REPORT', index=False)
    _buffer.seek(0)
        
    _res = s3.Object(bucket, path).put(Body=_buffer.getvalue())

    return list(_res.items())[0][1]['HTTPStatusCode'] == 200
    
def save_parquet_s3(s3, data, bucket, path):
        
    _buffer = BytesIO()
    
    data.to_parquet(_buffer)
    _buffer.seek(0)
        
    _res = s3.Object(bucket, path).put(Body=_buffer.getvalue())

    return list(_res.items())[0][1]['HTTPStatusCode'] == 200
    
def read_parquet_s3( s3_api, path:str, bucket:str):
    obj_buffer = s3_api.Object(bucket, path)

    with BytesIO(obj_buffer.get()['Body'].read()) as buffer_fd:
        _df = pd.read_parquet(buffer_fd)
    
    return _df

def read_csv_s3( s3_api, path:str, bucket:str):
    obj_buffer = s3_api.Object(bucket, path)

    with BytesIO(obj_buffer.get()['Body'].read()) as buffer_fd:
        _df = pd.read_csv(buffer_fd, compression='gzip', sep=';')
    
    return _df



@task(
    executor_config={'LocalExecutor': {}},
)
def get_dates(ds=None, data_interval_start=None, data_interval_end=None, ti=None, yesterday_ds=None,  **kwargs):
   
    
    print(yesterday_ds)
    _year = yesterday_ds.split('-')[0]
    _month = yesterday_ds.split('-')[1]
    _day = yesterday_ds.split('-')[2]
    _prefix = "%s%s%s"%(_year,_month,_day)
    _prefix_date = "%s-%s-%s"%(_year,_month,_day)
    
    _file_nokia = '%s/%s/REPORT_NOKIA_%s.parquet'%(DIR_NOKIA, _year,_prefix)
    _file_huawei = '%s/%s/REPORT_HUAWEI_%s.parquet'%(DIR_HUAWEI, _year,_prefix)
    _file_out_parquet = '%s/%s/REPORT_ACCESS_%s_v%s.parquet'%(DIR_OUT, _year, _prefix, VERSION)
    _file_out_xlsx = '%s/%s/REPORT_ACCESS_%s_v%s.xlsx'%(DIR_OUT, _year, _prefix,  VERSION)
    
    
    
    print("File Nokia: %s"%_file_nokia)
    print("File Huawei: %s"%_file_huawei)
    print("File out parquet: %s"%_file_out_parquet)
    print("File out excel: %s"%_file_out_xlsx)

    
    ti.xcom_push(key='file_nokia', value=_file_nokia)
    ti.xcom_push(key='file_huawei', value=_file_huawei)
    ti.xcom_push(key='file_out_parquet', value=_file_out_parquet)
    ti.xcom_push(key='file_out_xlsx', value=_file_out_xlsx)
    ti.xcom_push(key='prefix_date', value=_prefix_date)
    
    
    return True

@task(
    executor_config={'LocalExecutor': {}},
)
def check_files( ti=None,  **kwargs):
    
    _file_nokia  =  ti.xcom_pull(task_ids='get_dates', key='file_nokia')
    _file_huawei  =  ti.xcom_pull(task_ids='get_dates', key='file_huawei')
    
    
    _s3_api = boto3.resource('s3',
        aws_access_key_id = ACCESS_KEY,
        aws_secret_access_key = SECRET_KEY,
        region_name = REGION, 
        endpoint_url = ENDPOINT 
    )
    
    _bucket = _s3_api.Bucket(BUCKET)
    
    _list_files = list(_bucket.objects.filter(Prefix=_file_huawei))
    if(len(_list_files)>0):
        print("File %s  exist"%_file_huawei)
    else:
        raise AirflowException("File %s   not exist"%_file_huawei)


 
    _list_files = list(_bucket.objects.filter(Prefix=_file_nokia))
    if(len(_list_files)>0):
        print("File %s  exist"%_file_nokia)
    else:
        raise AirflowException("File %s   not exist"%_file_nokia)


    return True
  

@task(
    executor_config={'LocalExecutor': {}},
)
def cooncat_files( ti=None,  **kwargs):

    _file_nokia = ti.xcom_pull(task_ids='get_dates', key='file_nokia')
    _file_huawei = ti.xcom_pull(task_ids='get_dates', key='file_huawei')
    _file_out_parquet = ti.xcom_pull(task_ids='get_dates', key='file_out_parquet')
    _file_out_xlsx = ti.xcom_pull(task_ids='get_dates', key='file_out_xlsx')
    _prefix_date = ti.xcom_pull(task_ids='get_dates', key='prefix_date')

    _s3_api = boto3.resource('s3',
        aws_access_key_id = ACCESS_KEY,
        aws_secret_access_key = SECRET_KEY,
        region_name = REGION, 
        endpoint_url = ENDPOINT 
    )
    
    print("Reading file: %s"%_file_huawei)
    HUAWEI_DF = read_parquet_s3(_s3_api, _file_huawei, BUCKET)
    HUAWEI_DF['VENDOR'] = 'HUAWEI'
    
    print("Reading file: %s"%_file_nokia)
    NOKIA_DF = read_parquet_s3(_s3_api, _file_nokia, BUCKET)
    NOKIA_DF['VENDOR'] = 'NOKIA'
    
    REPORT = pd.concat([NOKIA_DF,HUAWEI_DF])
    REPORT['TECH'] = REPORT['TECH'].str.upper()
    REPORT['ENODEID'] = pd.to_numeric(REPORT.ENODEID).fillna(0)
    REPORT['VOL'] = REPORT['VOL'].fillna(0)
    REPORT['THRPUT'] = REPORT['THRPUT'].fillna(0)
    REPORT['CCUSERS'] = REPORT['CCUSERS'].fillna(0)
    REPORT['USER_THRPUT'] = REPORT['USER_THRPUT'].fillna(0)
    REPORT['TECH'] = REPORT['TECH'].str.upper()

    REPORT = REPORT[pd.notnull(REPORT['SITE'])]
    REPORT = REPORT[pd.notnull(REPORT['CELL'])]
    
    REPORT = REPORT.reset_index(drop=True)
    
    
    ## CREATING REPORT
    _report = REPORT.copy()
    _report_list = []


    _report_by_cell_by_hour = _report.copy()
    _report_by_cell_by_hour['TOTAL'] = 0
    _report_list.append(_report_by_cell_by_hour)

    _report_site_tech_hour = _report.groupby(['SITE','TECH','PERIOD_START_TIME'], as_index=False).agg({
        'VOL':'sum', 
        'THRPUT':'sum',
        'USER_THRPUT': 'mean',
        'CCUSERS':'max'
    })
    _report_site_tech_hour['TOTAL'] = 1
    _report_list.append(_report_site_tech_hour)

    _report_site_hour = _report.groupby(['SITE','PERIOD_START_TIME'], as_index=False).agg({
        'VOL':'sum', 
        'THRPUT':'sum',
        'USER_THRPUT': 'mean',
        'CCUSERS':'max'
    }) 
    _report_site_hour['TOTAL'] = 2
    _report_list.append(_report_site_hour)

    _report_cell = _report.groupby(['CELL'], as_index=False).agg({
        #'PERIOD_START_TIME':'min',
        'TECH':'max',
        'SITE': 'max',
        'VOL':'sum', 
        'THRPUT':'max',
        'USER_THRPUT': 'mean',
        'CCUSERS':'max'
    })
    _report_cell['TOTAL'] = 10
    _report_list.append(_report_cell)

    REPORT_AGG_SITE_0 = {
        'PERIOD_START_TIME':'min',
        'VOL':'sum', 
        'THRPUT':'sum',
        'USER_THRPUT': 'mean',
        'CCUSERS':'sum'
    }

    REPORT_AGG_SITE_1 = {
        #'PERIOD_START_TIME':'min',
        'VOL':'sum', 
        'THRPUT':'max',
        'USER_THRPUT': 'mean',
        'CCUSERS':'max'
    }
    print()
    print("Grooping by Site")
    _report_site = _report.groupby(['SITE','PERIOD_START_TIME'], group_keys=False).agg(REPORT_AGG_SITE_0)
    _report_site = _report_site.groupby(level=0, group_keys=False).agg(REPORT_AGG_SITE_1)
    _report_site.reset_index(inplace=True)
    _report_site['TOTAL'] = 11
    _report_list.append(_report_site)

    print()
    print("Grooping by Site and Tech")
    _report_site_tech = _report.groupby(['SITE','PERIOD_START_TIME','TECH'], group_keys=False).agg(REPORT_AGG_SITE_0)
    _report_site_tech = _report_site_tech.groupby(level=[0,2], group_keys=False).agg(REPORT_AGG_SITE_1)
    _report_site_tech.reset_index(inplace=True)
    _report_site_tech['TOTAL'] = 12
    _report_list.append(_report_site_tech)

    REPORT_AGG_SITE_2 = {
        'VOL':'sum', 
        'THRPUT':'sum',
        'USER_THRPUT': 'mean',
        'CCUSERS':'sum'
    }
    print()
    print("Grouping All by Tech")
    _report_all_tech = _report.groupby(['PERIOD_START_TIME','TECH'], group_keys=False).agg(REPORT_AGG_SITE_2)
    _report_all_tech = _report_all_tech.groupby('TECH').resample('D', level=0).agg({'VOL':'sum','THRPUT':'max','CCUSERS':'sum'})
    _report_all_tech.reset_index(inplace=True)
    _report_all_tech = _report_all_tech.drop('PERIOD_START_TIME', axis=1)
    _report_all_tech['TOTAL'] = 13
    _report_list.append(_report_all_tech)


    print()
    print("Grouping All")
    _report_all = _report.groupby(['PERIOD_START_TIME'], group_keys=False).agg(REPORT_AGG_SITE_2)
    _report_all = _report_all.resample('D').agg({'VOL':'sum','THRPUT':'max','CCUSERS':'sum'})
    _report_all.reset_index(inplace=True)
    _report_all = _report_all.drop('PERIOD_START_TIME', axis=1)
    _report_all['TOTAL'] = 14
    _report_list.append(_report_all)

    _report = pd.concat(_report_list)
    _report = _report[[
        'SITE', 
        'CELL', 
        'TECH', 
        'PERIOD_START_TIME', 
        'VOL', 
        'CCUSERS', 
        'THRPUT',
        'USER_THRPUT',
        'TOTAL']
    ]
    
    _report['PERIOD_START_TIME'] = _prefix_date
        
    print("Uploading report to file XLS: %s"%_file_out_xlsx)
    save_excel_s3(_s3_api, _report[_report.TOTAL > 9], BUCKET, _file_out_xlsx)
    
    print("Uploading report to file PARQUET: %s"%_file_out_parquet)
    save_parquet_s3(_s3_api, _report, BUCKET, _file_out_parquet)
    
    return True
    

@task.sensor(
    poke_interval=60, 
    timeout=3600, 
    mode="reschedule"
)
def check_s3_file(ti=None,  **kwargs):

    _file_nokia = ti.xcom_pull(task_ids='get_dates', key='file_nokia')
    _file_huawei = ti.xcom_pull(task_ids='get_dates', key='file_huawei')
    
    _s3_api = boto3.resource('s3',
        aws_access_key_id = ACCESS_KEY,
        aws_secret_access_key = SECRET_KEY,
        region_name = REGION, 
        endpoint_url = ENDPOINT 
    )
    
    print("Esperando archivo: %s"%_file_nokia)
    print("Esperando archivo: %s"%_file_huawei)

    _bucket = _s3_api.Bucket(BUCKET)
    

    _file_exists_huawei = len(list(_bucket.objects.filter(Prefix=_file_huawei))) > 0 
    _file_exists_nokia = len(list(_bucket.objects.filter(Prefix=_file_nokia))) > 0


    if _file_exists_huawei & _file_exists_nokia:
        print("Archivos encontrados")
        return PokeReturnValue(is_done=True, xcom_value="xcom_value")
       
    print("Archivo encontrado: %s    %s"%(_file_nokia,_file_exists_nokia))
    print("Archivo encontrado: %s    %s"%(_file_huawei,_file_exists_huawei))
    
    return PokeReturnValue(is_done=False, xcom_value="xcom_value")



with DAG(
    dag_id='report_huawei_nokia',
    schedule_interval= "@daily",
    default_args={
        "depends_on_past": False,
        'owner': 'Vmasuero',
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    start_date=pendulum.datetime( 2024, 11, 11, tz='America/Santiago'),
    catchup=True,
    tags=['development', 'netact', 'huawei'],
    max_active_runs=1
    ) as dag:
    
        get_dates() >> check_s3_file() #>> check_files() >> cooncat_files()