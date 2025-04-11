import pendulum
import pandas as pd
import oracledb
from datetime import datetime, timedelta
import os
import re
import tempfile
import boto3 

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.decorators import dag, task
from airflow.utils.helpers import chain
from airflow.providers.sftp.hooks.sftp import SFTPHook
from io import StringIO,BytesIO
from dask.distributed import Client, LocalCluster, Future, as_completed, fire_and_forget



SECRET_KEY ='2DhT3mGRLmNDBOl9ZuxCLdic0jXSmfUiZ+niJrwp3cU='
ACCESS_KEY = 'd7556c3cc7c1996477a5c851b51e2f47ea4d00a6'
REGION = 'sa-santiago-1'
NAMESPACE = 'axosppplfddw'
BUCKET = 'bucket-scl-prod-monitoreosscc-datalake-001'
ENDPOINT = "https://%s.compat.objectstorage.%s.oraclecloud.com"%(NAMESPACE,REGION)
bucket_url = f'https://objectstorage.{REGION}.oraclecloud.com/n/{NAMESPACE}/b/{BUCKET}/o/arieso/tmp/file_arieso.csv'


CLUSTER_DASK_IP = 'dask-cluster-airflow-scheduler.dask-cluster:8786'

HOURS_DELAY = 6
DAYS_DELAY = 0

NETACT_TABLES_COUNTERS = [

    #2G
    #'BSC_P_RESAVAIL_BTS_HOUR',
    #'BSC_P_RESACC_BTS_HOUR',
    #'BSC_P_SERVICE_BTS_HOUR',
    #'BSC_P_HO_BTS_HOUR',
    #'BSC_P_TRAFFIC_BTS_HOUR',
    #'BSC_P_PCU_BTS_HOUR',
    #'BSC_P_CODINGSC_BTS_HOUR',
    #'BSC_P_FER_BTS_HOUR',
    #'BSC_P_HOADJ_CI1_DAY',
    #'BSC_P_RXQUAL_BTS_HOUR',

    #3G
    'NOKRWW_P_CELLRES2_WCEL_HOUR',
    'NOKRWW_P_SERVLEV4_WCEL_HOUR',
    'NOKRWW_P_SERVLEV1_WCEL_HOUR',
    'NOKRWW_P_SERVLEV3_WCEL_HOUR',
    'NOKRWW_P_SERVLEV2_WCEL_HOUR',
    'NOKRWW_P_PKTCALL_WCEL_HOUR',
    'NOKRWW_P_CELTPW_WCEL_HOUR',
    'NOKRWW_P_HSDPAW2_WCEL_HOUR',
    'NOKRWW_P_CELLTP_WCEL_HOUR',
    'NOKRWW_P_HSDPAW1_WCEL_HOUR',
    'NOKRWW_P_CELLRES1_WCEL_HOUR',
    'NOKRWW_P_SOFTHO_WCEL_HOUR',
    'NOKRWW_P_INTERSHO_WCEL_HOUR',
    #'NOKRWW_P_INTSYSHO_WCEL_HOUR',



    #4G
    'NOKLTE_P_LCELLT_LNCEL_HOUR',
    'NOKLTE_P_LUEQ_LNCEL_HOUR',
    'NOKLTE_P_LCELLR_LNCEL_HOUR',
    'NOKLTE_P_LPQDL_LNCEL_HOUR',
    'NOKLTE_P_LCELAV_LNCEL_HOUR',
    'NOKLTE_P_LCELLD0_LNCEL_HOUR',
    'NOKLTE_P_LCELLD2_LNCEL_HOUR',
    'NOKLTE_P_LCELLD1_LNCEL_HOUR',
    'NOKLTE_P_LRRC_LNCEL_HOUR',
    'NOKLTE_P_LUEST_LNCEL_HOUR',
    'NOKLTE_P_LEPSB_LNCEL_HOUR',
    'NOKLTE_P_LISHO_LNCEL_HOUR',
    'NOKLTE_P_LIANBHO_LNCEL_HOUR',
    'NOKLTE_P_LIENBHO_LNCEL_HOUR',
    'NOKLTE_P_LHO_LNCEL_HOUR',
    'NOKLTE_P_LPQUL1_LNCEL_HOUR',
    'NOKLTE_P_LMAC_LNCEL_HOUR',
    'NOKLTE_P_LPQUL2_LNCEL_HOUR',
    

    #5G
    'NOKGNB_P_NCELA_NRCEL_HOUR',
    'NOKGNB_P_NMSDU_NRCEL_HOUR',
    'NOKGNB_P_RACCU_NRCEL_HOUR',
    'NOKGNB_P_NMPDU_NRCEL_HOUR',
    'NOKGNB_P_NCAV_NRCEL_HOUR',
    'NOKGNB_P_NF1CC_NRCEL_HOUR',
    'NOKGNB_P_NX2CC_NRCEL_HOUR',
    'NOKGNB_P_NRACH_NRCEL_HOUR',
    'NOKGNB_P_NNSAU_NRCEL_HOUR',
    'NOKGNB_P_NINFC_NRCEL_HOUR',
    'NOKGNB_P_NDLHQ_NRCEL_HOUR',
    'NOKGNB_P_NULHQ_NRCEL_HOUR',
    'NOKGNB_P_NDLSQ_NRCEL_HOUR',
    'NOKGNB_P_NULSQ_NRCEL_HOUR',
    'NOKGNB_P_NRTA_NRCEL_HOUR',

    #TWAMPS
    'NOKMRN_PS_TWAM_TWAMP_RAW',
]

RAW_COUNTERS_IDX_COLS = [
    'CO_GID',
    'PERIOD_START_TIME',
]

RAW_COUNTERS_3G_COLS = [ 
    #'CO_GID',
    #'RNC_ID',
    #'WBTS_ID',
    #'WCEL_ID',
    #'PERIOD_START_TIME',
    'MAX_HSDPA_USERS_IN_CELL',
    'RNC_645C',
    'RRC_CONNECTED_UE_MAX',
    'RRC_CONN_UE_MAX',
    'RECEIVED_HS_MACD_BITS',
    'MC_HSDPA_ORIG_DATA_PRI',
    'MC_HSDPA_ORIG_DATA_SEC',
    'HSDPA_BUFF_WITH_DATA_PER_TTI',
    'HSDPA_ORIG_DATA',
    'HS_SCCH_PWR_DIST_CLASS_0',
    'HS_SCCH_PWR_DIST_CLASS_1',
    'HS_SCCH_PWR_DIST_CLASS_2',
    'HS_SCCH_PWR_DIST_CLASS_3',
    'HS_SCCH_PWR_DIST_CLASS_4',
    'HS_SCCH_PWR_DIST_CLASS_5',
    'DISCARDED_HS_MACD_BITS'
]

RAW_COUNTERS_4G_COLS = [
    #'MRBTS_ID',
    #'LNBTS_ID',
    #'LNCEL_ID',
    #'PERIOD_START_TIME',
    'PDCP_SDU_VOL_DL',
    'PDCP_DATA_RATE_MAX_DL',
    'VOL_ORIG_TRANS_DL_SCH_TB',
    'IP_TPUT_VOL_DL_QCI_1',
    'IP_TPUT_TIME_DL_QCI_1',
    'IP_TPUT_VOL_DL_QCI_2',
    'IP_TPUT_TIME_DL_QCI_2',
    'IP_TPUT_VOL_DL_QCI_3',
    'IP_TPUT_TIME_DL_QCI_3',
    'IP_TPUT_VOL_DL_QCI_4',
    'IP_TPUT_TIME_DL_QCI_4',
    'IP_TPUT_VOL_DL_QCI_5',
    'IP_TPUT_TIME_DL_QCI_5',
    'IP_TPUT_VOL_DL_QCI_6',
    'IP_TPUT_TIME_DL_QCI_6',
    'IP_TPUT_VOL_DL_QCI_7',
    'IP_TPUT_TIME_DL_QCI_7',
    'IP_TPUT_VOL_DL_QCI_8',
    'IP_TPUT_TIME_DL_QCI_8',
    'IP_TPUT_VOL_DL_QCI_9',
    'IP_TPUT_TIME_DL_QCI_9',
    'ACTIVE_TTI_DL',
    'DL_PRB_UTIL_TTI_MEAN',
    'PRB_USED_DL_TOTAL',
    'PRB_USED_PDSCH',
    'MEAN_PRB_AVAIL_PDSCH',
    'DL_PRB_UTIL_TTI_MEAN_LTEM',
    'RRC_CONNECTED_UE_AVG',
    'RRC_CONNECTED_UE_MAX'
]

RAW_COUNTERS_5G_COLS = [
    #'MRBTS_GID', 
    #'NRBTS_GID', 
    #'NRCEL_GID',
    #'PERIOD_START_TIME',
    'UL_MAC_SDU_VOL_DTCH',
    'DL_MAC_SDU_VOL_DTCH',
    'ACC_SCHED_UE_PDSCH', 
    'ACC_UE_DL_DRB_DATA', 
    'ACC_UE_UL_DRB_DATA',
    'DATA_SLOT_PDSCH', 
    'DATA_SLOT_PDSCH_TIME', 
    'DATA_SLOT_PUSCH',
    'DATA_SLOT_PUSCH_TIME', 
    #'PDSCH_INI_VOL_64TBL_MCS00', 
    #'PDSCH_INI_VOL_64TBL_MCS02',
    #'PDSCH_INI_VOL_64TBL_MCS04', 
    #'PDSCH_INI_VOL_64TBL_MCS06',
    #'PDSCH_INI_VOL_64TBL_MCS08', 
    #'PDSCH_INI_VOL_64TBL_MCS10',
    #'PDSCH_INI_VOL_64TBL_MCS12', 
    #'PDSCH_INI_VOL_64TBL_MCS14',
    #'PDSCH_INI_VOL_64TBL_MCS16', 
    #'PDSCH_INI_VOL_64TBL_MCS18',
    #'PDSCH_INI_VOL_64TBL_MCS20', 
    #'PDSCH_INI_VOL_64TBL_MCS22',
    #'PDSCH_INI_VOL_64TBL_MCS24',
    #'PDSCH_INI_VOL_64TBL_MCS26',
    #'PDSCH_INI_VOL_64TBL_MCS28',
    #'PDSCH_INI_VOL_256TBL_MCS01', 
    #'PDSCH_INI_VOL_256TBL_MCS03',
    #'PDSCH_INI_VOL_256TBL_MCS05', 
    #'PDSCH_INI_VOL_256TBL_MCS07',
    #'PDSCH_INI_VOL_256TBL_MCS09', 
    #'PDSCH_INI_VOL_256TBL_MCS11',
    #'PDSCH_INI_VOL_256TBL_MCS13',
    #'PDSCH_INI_VOL_256TBL_MCS15',
    #'PDSCH_INI_VOL_256TBL_MCS17', 
    #'PDSCH_INI_VOL_256TBL_MCS19',
    #'PDSCH_INI_VOL_256TBL_MCS21', 
    #'PDSCH_INI_VOL_256TBL_MCS23',
    #'PDSCH_INI_VOL_256TBL_MCS25', 
    #'PDSCH_INI_VOL_256TBL_MCS27',   
    'PRB_AVAIL_PDSCH', 
    'PRB_AVAIL_PUSCH',
    'TRS_SLOT_PDSCH', 
    'MEAS_PERIOD_NR_MAC_PDU_TPUT', 
    'PEAK_NUMBER_OF_NSA_USERS', 
]

RAW_COUNTERS_CO_GID = [
    'NRCEL_GID',
    'LNCEL_ID',
    'WCEL_ID' 
]

RAW_COUNTERS_COLS = RAW_COUNTERS_IDX_COLS + RAW_COUNTERS_CO_GID + RAW_COUNTERS_3G_COLS + RAW_COUNTERS_4G_COLS + RAW_COUNTERS_5G_COLS
RAW_COUNTERS_COLS_NUM = RAW_COUNTERS_3G_COLS + RAW_COUNTERS_4G_COLS + RAW_COUNTERS_5G_COLS


NETACT_TABLES_STATIC = [
    'UTP_COMMON_OBJECTS',
    #'C_LTE_LNCEL_FDD',
    #'C_LTE_LNBTS',
    #'OBJREG_REGISTRY_OBJECT'
]

NETACT_IPS = ['172.16.42.133', '172.16.52.133']

SAMPLING = 60
NOKIA_COUNTERS_PATH = 'Nokia/Counters_%smin'%SAMPLING
NOKIA_COUNTERS_PATH_STATIC = 'Nokia/Static'

def save_csv_s3(s3,data, bucket, path):
        
    _buffer = BytesIO()
    
    data.to_csv(_buffer, compression = "gzip", sep=";", index=False)
    _buffer.seek(0)
    
    _res = s3.Object(bucket, path).put(Body=_buffer.getvalue())
    
    return list(_res.items())[0][1]['HTTPStatusCode'] == 200
    
def save_parquet_s3(s3, data, bucket, path):
        
    _buffer = BytesIO()
    
    data.to_parquet(_buffer)
    _buffer.seek(0)
    
    #s3.put_object(Bucket=bucket, Key=path, Body=_buffer.getvalue())
    
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
def get_dates(ds=None, data_interval_start=None, data_interval_end=None, ti=None,  **kwargs):
   
    
    _time_start = pd.to_datetime(data_interval_start)
    _time_start = _time_start - timedelta(days=DAYS_DELAY)
    _time_start = _time_start - timedelta(hours=HOURS_DELAY)

    _date_ini = _time_start.replace(minute=0, second=0).strftime('%d-%m-%Y %H:%M:%S')
    _date_end = _time_start.replace(minute=59, second=59).strftime('%d-%m-%Y %H:%M:%S')
    
    _date_ini_next = (_time_start.replace(minute=0, second=0) + timedelta(hours=1)).strftime('%d-%m-%Y %H:%M:%S')
    _date_end_next = (_time_start.replace(minute=59, second=59) + timedelta(hours=1)).strftime('%d-%m-%Y %H:%M:%S')

    _hour_oper = _time_start.hour
    
    
    print(_time_start)
    print(_time_start.isocalendar())
    
    _date_prefix_by_hour = "%s%s%s%s"%(
        _time_start.year,
        str(_time_start.month).zfill(2),
        str(_time_start.day).zfill(2),
        str(_time_start.hour).zfill(2)
    )
   
    _path_out_dir = "%s/%s/%s/%s"%(
        NOKIA_COUNTERS_PATH,
        _time_start.year,
        str(_time_start.month).zfill(2),
        str(_time_start.day).zfill(2)
        )
        
    _path_out_dir_tmp = "%s/tmp"%(_path_out_dir)

    _path_out_dir_static = "%s/%s/%s/%s"%(
        NOKIA_COUNTERS_PATH_STATIC,
        _time_start.year,
        str(_time_start.month).zfill(2),
        str(_time_start.day).zfill(2)
        )
        
    print("Date in RAW version: %s "%ds)
    print("Init date: %s"%_date_ini)
    print("End date: %s"%_date_end)
    

    ti.xcom_push(key='date_ini', value=_date_ini)
    ti.xcom_push(key='date_end', value=_date_end)
    ti.xcom_push(key='date_ini_next', value=_date_ini_next)
    ti.xcom_push(key='date_end_next', value=_date_end_next)
    #ti.xcom_push(key='File Prefix', value=_prefix_file)
    ti.xcom_push(key='date_prefix_by_hour', value=_date_prefix_by_hour)
    ti.xcom_push(key='path_out_dir', value=_path_out_dir)
    ti.xcom_push(key='path_out_dir_static', value=_path_out_dir_static)
    ti.xcom_push(key='path_out_dir_tmp', value=_path_out_dir_tmp)
    ti.xcom_push(key='hour_oper', value=_hour_oper)
    
    
    return True

def dw_table_counters(table_name:str, ds=None, ti=None,  **kwargs):

    def get_table(table_name:str, sql_query:str):

        _dfs = []
        for i,netact_ip in enumerate(NETACT_IPS):
            _rows = []
            with oracledb.connect(user='rdr', password='rdr', dsn='%s/OSS'%netact_ip) as connection:
                with connection.cursor() as cursor:
                    #sql="SELECT * FROM %s FETCH FIRST 5 ROWS ONLY"%table_name
                    try:
                        for r in cursor.execute(sql_query):
                            _rows.append(r)
                    except Exception as e:
                        print(e)
                        print("Error con la interaccion con la Base de Datos")
                        raise AirflowException

            _dfs.append( pd.DataFrame(_rows, columns=get_columns_by_netact(table_name, i))   )

        return pd.concat(_dfs)

    def check_data_available(table_name:str, date_ini_next, date_end_next):
        print("Checking available info: %s"%table_name)
        
        _sql_query = "SELECT * FROM %s WHERE period_start_time  BETWEEN to_date('%s', 'DD-MM-YYYY HH24:MI:SS') AND to_date('%s','DD-MM-YYYY HH24:MI:SS') FETCH FIRST 10 ROWS ONLY"%(
            table_name,
            date_ini_next, 
            date_end_next
        )
        print("SQL query for checking next hour data:")
        print(_sql_query)
        _table = get_table(table_name, _sql_query)
        
        if not _table.empty:
            return True
            
        return False
        
    def get_columns_by_netact(table_name:str, netact_id:int):
        _columns = []
        with oracledb.connect(user='rdr', password='rdr', dsn='%s/OSS'%NETACT_IPS[netact_id]) as connection:
            with connection.cursor() as cursor:
            
                _sql = "select * from ALL_TAB_COLUMNS where TABLE_NAME='%s'"%table_name    
                try:
                    _cols = [{'table':r[1], 'columns':r[2], 'order': r[10],  'NetAct':netact_id}  for r in cursor.execute(_sql)]
                except Exception as e:
                    print("Error con interaccion con la Base de Datos")
                    return []

        return pd.DataFrame(_cols).sort_values(by='order')['columns'].to_list()
  
    _s3_api = boto3.resource('s3',
        aws_access_key_id = ACCESS_KEY,
        aws_secret_access_key = SECRET_KEY,
        region_name = REGION, 
        endpoint_url = ENDPOINT 
    )
    
    print("Extarcting info from table: %s"%table_name)
    _date_ini = ti.xcom_pull(task_ids="get_dates", key="date_ini")
    _date_end = ti.xcom_pull(task_ids="get_dates", key="date_end")
    _date_ini_next = ti.xcom_pull(task_ids="get_dates", key="date_ini_next")
    _date_end_next = ti.xcom_pull(task_ids="get_dates", key="date_end_next")     
    
    _path_out_dir = ti.xcom_pull(task_ids="get_dates", key="path_out_dir")
    _date_prefix_by_hour = ti.xcom_pull(task_ids="get_dates", key="date_prefix_by_hour")
    

    if check_data_available(table_name,_date_ini_next,_date_end_next):
        print("Data Available....................OK")
    else:
        raise AirflowException("Data is not Available yet....................NOK")

    _sql_query = "SELECT * FROM %s WHERE period_start_time  BETWEEN to_date('%s', 'DD-MM-YYYY HH24:MI:SS') AND to_date('%s','DD-MM-YYYY HH24:MI:SS') "%(table_name,_date_ini, _date_end)

    _filename = "%s/%s_%s.csv.gz"%(_path_out_dir,table_name,_date_prefix_by_hour)
         
    print("Exceuting: %s"%_sql_query)
    _table = get_table(table_name, _sql_query)
   

    if _table.empty:
        raise AirflowException('Sin informacion en la tabla SQL')

    print("Saving  Table %s in file %s "%(table_name,_filename) )
    save_csv_s3(_s3_api,_table, BUCKET, _filename)
    
    return True 

def dw_table_static(table_name:str, data_interval_start=None, data_interval_end=None, ti=None,  **kwargs):

    def get_columns_by_netact(table_name:str, netact_id:int):
        _columns = []
        with oracledb.connect(user='rdr', password='rdr', dsn='%s/OSS'%NETACT_IPS[netact_id]) as connection:
            with connection.cursor() as cursor:
            
                _sql = "select * from ALL_TAB_COLUMNS where TABLE_NAME='%s'"%table_name    
                try:
                    _cols = [{'table':r[1], 'columns':r[2], 'order': r[10],  'NetAct':netact_id}  for r in cursor.execute(_sql)]
                except Exception as e:
                    print("Error con interaccion con la Base de Datos")
                    return []

        return pd.DataFrame(_cols).sort_values(by='order')['columns'].to_list()


    def get_table(table_name:str, sql_query:str):

        _dfs = []
        for i,netact_ip in enumerate(NETACT_IPS):
            _rows = []
            with oracledb.connect(user='rdr', password='rdr', dsn='%s/OSS'%netact_ip) as connection:
                with connection.cursor() as cursor:
                    #sql="SELECT * FROM %s FETCH FIRST 5 ROWS ONLY"%table_name
                    try:
                        for r in cursor.execute(sql_query):
                            _rows.append(r)
                    except Exception as e:
                        print(e)
                        print("Error con la interaccion con la Base de Datos")
                        raise AirflowException

            _dfs.append( pd.DataFrame(_rows, columns=get_columns_by_netact(table_name, i))   )

        return pd.concat(_dfs)  

    
    _s3_api = boto3.resource('s3',
        aws_access_key_id = ACCESS_KEY,
        aws_secret_access_key = SECRET_KEY,
        region_name = REGION, 
        endpoint_url = ENDPOINT 
    )
    
    _hour_oper = ti.xcom_pull(task_ids="get_dates", key="hour_oper")
    _hour_oper = int(_hour_oper)
    print("Oper Hour: %s"%_hour_oper)
    
    _path_out_dir_static  = ti.xcom_pull(task_ids="get_dates", key="path_out_dir_static")
    
    if _hour_oper < 23:
        print('Wating for end of the day')
        raise AirflowSkipException('Wating for final of the day')

    
    print("Extarcting info from table: %s"%table_name)
    _sql_query="SELECT * FROM %s"%table_name
    
    print("Exceuting: %s"%_sql_query)
    _table = get_table(table_name, _sql_query)
    
    
    _filename_static = "%s/%s.csv.gz"%(_path_out_dir_static,table_name)
    print('Saving to file: %s'%_filename_static)
    save_csv_s3(_s3_api,_table, BUCKET, _filename_static)
    
    ti.xcom_push(key='_filename_static', value=_filename_static)
    
    
    return True

@task(
    executor_config={'LocalExecutor': {}},
)
def create_table_static(ds=None, data_interval_start=None, data_interval_end=None, ti=None,  **kwargs):

    def proc_co_dn(line:str):
            _data = line.split('/')
            _ret  = {x.split('-')[0]:x.split('-')[1] for x in _data[1:]}

            return _ret
        
    def indentify_site(name:str):
    
        if re.match('(\d+)([ispISP])?.*(\d\d\d)[:-]?(\d+)?',name):
            _m = re.search('(\d+)([ispISP])?.*(\d\d\d)[:-]?(\d+)?',name)
            x1,x2,x3,x4 = _m.groups()
            return "%s%s_%s"%(
                            x1,
                            '' if x2 == None else x2,
                            x3)

        if re.match('([A-Z]+)(\d+)?[A-Z_](\d\d\d)[-:]?(\d+)?',name):
            _m = re.search('([A-Z]+)(\d+)?[A-Z_](\d\d\d)[-:]?(\d+)?',name)
            x1,x2,x3,x4 = _m.groups()
            return "%s%s_%s"%(
                            x1,
                            '' if x2 == None else x2,
                            x3)

        return 'unknow'
        
    _hour_oper = ti.xcom_pull(task_ids="get_dates", key="hour_oper")
    _hour_oper = int(_hour_oper)
    print("Oper Hour: %s"%_hour_oper)
    
    if _hour_oper < 23:
        print('Wating for end of the day')
        raise AirflowSkipException('Wating for final of the day')
  
    _s3_api = boto3.resource('s3',
    aws_access_key_id = ACCESS_KEY,
    aws_secret_access_key = SECRET_KEY,
    region_name = REGION, 
    endpoint_url = ENDPOINT 
    )

    
    print("Static file proceesing")
    _table_name = 'UTP_COMMON_OBJECTS'
    _path_out_dir_static  = ti.xcom_pull(task_ids="get_dates", key="path_out_dir_static")
    _path_out_dir_tmp  = ti.xcom_pull(task_ids="get_dates", key="path_out_dir_tmp")
    _filename_static = "%s/%s.csv.gz"%(_path_out_dir_static,_table_name)
    _filname_static_tmp = "%s/%s_tmp.parquet"%(_path_out_dir_tmp,_table_name)
    
    _utp_common_df = read_csv_s3(_s3_api, _filename_static, BUCKET)    
    _utp_common_df = _utp_common_df[ ['CO_GID','CO_DN','CO_NAME'] ]
    _utp_common_df = _utp_common_df[pd.notnull(_utp_common_df['CO_DN'])]
    _utp_common_df = _utp_common_df[pd.notnull(_utp_common_df['CO_NAME'])]

    print('Analysing CO NAME')
    _utp_common_df = _utp_common_df.join(
        pd.DataFrame([ pd.Series(proc_co_dn(it[1]), name=it[0]) for it in _utp_common_df.CO_DN.items() ] )
    )
    _utp_common_df = _utp_common_df[pd.notnull(_utp_common_df.NRCEL) | pd.notnull(_utp_common_df.LNCEL) | pd.notnull(_utp_common_df.WCEL)]

    print('Idenfity TECH')
    _utp_common_df.loc[ pd.notnull(_utp_common_df.NRBTS),'TECH'] = '5G'
    _utp_common_df.loc[ pd.isnull(_utp_common_df.NRBTS) & pd.notnull(_utp_common_df.MRBTS),'TECH'] = '4G'
    _utp_common_df.loc[ pd.notnull(_utp_common_df.RNC),'TECH'] = '3G' 
    _utp_common_df.loc[ pd.notnull(_utp_common_df.BSC),'TECH'] = '2G'

    _utp_common_df = _utp_common_df[pd.notnull(_utp_common_df.TECH)]
    _utp_common_df.reset_index(inplace=True)
    _utp_common_df = _utp_common_df.drop('index', axis=1)

    _utp_common_df['SITE'] = _utp_common_df.CO_NAME.apply(indentify_site)
    _utp_common_df = _utp_common_df[_utp_common_df.SITE != 'unknow']
    _utp_common_df = _utp_common_df[['CO_GID','SITE','CO_NAME','TECH']]
    
    print('Cleaning data')
    _utp_common_df = _utp_common_df[ ~_utp_common_df.SITE.str.match(r'^730.*')]
    _utp_common_df = _utp_common_df[ ~_utp_common_df.CO_GID.duplicated()]
    
    print("Saving in tmp file: %s"%_filname_static_tmp)
    save_parquet_s3(_s3_api, _utp_common_df, BUCKET, _filname_static_tmp)
    ti.xcom_push(key='fileame_static_tmp', value=_filname_static_tmp)
    
    return True
    
@task(
    executor_config={'LocalExecutor': {}},
)
def create_table_counters(ti=None,  **kwargs):

    _s3_api = boto3.resource('s3',
        aws_access_key_id = ACCESS_KEY,
        aws_secret_access_key = SECRET_KEY,
        region_name = REGION, 
        endpoint_url = ENDPOINT 
    )
    
    def proc_csv_gz(path:str, bucket:str):  
    
        _s3_api_1 = boto3.resource('s3',
            aws_access_key_id = ACCESS_KEY,
            aws_secret_access_key = SECRET_KEY,
            region_name = REGION, 
            endpoint_url = ENDPOINT 
        )
        
        def read_csv_s3_1( s3_api, path:str, bucket:str):
            obj_buffer = s3_api.Object(bucket, path)

            with BytesIO(obj_buffer.get()['Body'].read()) as buffer_fd:
                _df = pd.read_csv(buffer_fd, compression='gzip', sep=';')
            
            return _df    
            
            
        
        TABLE_NAME = path.split('/')[-1].split('_HOUR_')[0]
        
        _data = read_csv_s3_1(_s3_api_1, path, bucket)
        _num_columns =  [x for x in _data.columns if x.upper() in RAW_COUNTERS_COLS_NUM]
        _change_names = {x:"%s_1&1_%s"%(TABLE_NAME,x) for x in _num_columns}
        
        if len(_num_columns) == 0:
            return pd.DataFrame()
        
        _data = _data[
            [x for x in _data.columns if x in RAW_COUNTERS_COLS]
        ]
        _data['PERIOD_START_TIME'] = pd.to_datetime(_data['PERIOD_START_TIME'], errors='coerce', format='%Y-%m-%d %H:%M:%S')
        _data = _data[pd.notnull(_data['PERIOD_START_TIME'])]

        if 'NRCEL_GID' in _data.columns:
            _data = _data.rename(columns={'NRCEL_GID':'CO_GID'})

        if 'LNCEL_ID' in _data.columns:
            _data = _data.rename(columns={'LNCEL_ID':'CO_GID'})

        if 'WCEL_ID' in _data.columns:
            _data = _data.rename(columns={'WCEL_ID':'CO_GID'})

        _data = _data.rename(columns=_change_names)
        
        return _data

    _path_out_dir = ti.xcom_pull(task_ids="get_dates", key="path_out_dir")
    _path_out_dir_tmp = ti.xcom_pull(task_ids="get_dates", key="path_out_dir_tmp")
    
    

    FILES_ALL = [x.key for x in _s3_api.Bucket(BUCKET).objects.filter(Prefix=_path_out_dir) ]
    FILES_ALL = [x for x in FILES_ALL if re.match(r'.*\.csv\.gz', x)]
    
    print(FILES_ALL)
    
    DATA_COUNTERS = pd.DataFrame()        
    with Client(CLUSTER_DASK_IP) as DASK_CLIENT:
        futures_0 = DASK_CLIENT.map(proc_csv_gz, FILES_ALL, [BUCKET for x in FILES_ALL] )

        futures_1 = DASK_CLIENT.submit(pd.concat, futures_0)

        DATA_COUNTERS = futures_1.result()
        
  
    DATA_COUNTERS = DATA_COUNTERS.groupby(['CO_GID','PERIOD_START_TIME']).max()
    
    _fix_col_table = pd.DataFrame( [{'col':x.split('_1&1_')[1], 'col_tmp':x} for x in DATA_COUNTERS.columns])
    for k,v in _fix_col_table.groupby('col'):
        DATA_COUNTERS[k] = DATA_COUNTERS[list(v.col_tmp.values)].max(axis=1)
        
    DATA_COUNTERS = DATA_COUNTERS[ [x for x in DATA_COUNTERS.columns if '_1&1_'not in x] ] 
    
    _filename_counter = "%s/COUNTERS_tmp.parquet"%(_path_out_dir_tmp)
    print("savinf %s rows"%len(DATA_COUNTERS))
    print('Saving to file: %s'%_filename_counter)
    save_parquet_s3(_s3_api, DATA_COUNTERS, BUCKET, _filename_counter)
    
    ti.xcom_push(key='filename_counter', value=_filename_counter)
    
    
    return True

@task(
    executor_config={'LocalExecutor': {}},
)
def create_diary_report(ti=None,  **kwargs):

    _s3_api = boto3.resource('s3',
        aws_access_key_id = ACCESS_KEY,
        aws_secret_access_key = SECRET_KEY,
        region_name = REGION, 
        endpoint_url = ENDPOINT 
    )

    _filename_counter =  ti.xcom_pull(task_ids='create_table_counters', key='filename_counter')
    _filename_static_tmp =  ti.xcom_pull(task_ids='create_table_static', key='fileame_static_tmp')
    _date_prefix_by_hour =  ti.xcom_pull(task_ids='get_dates', key='date_prefix_by_hour')
    _year_prefix = _date_prefix_by_hour[:4]
    _date_prefix_by_day = _date_prefix_by_hour[:-2]
    
    print(_filename_counter)
    print(_filename_static_tmp)
       
    COUNTERS = read_parquet_s3(_s3_api, _filename_counter, BUCKET)
    COUNTERS.reset_index(inplace = True)
    COUNTERS = COUNTERS.set_index('CO_GID')
    COUNTERS = COUNTERS.rename(columns={'CO_NAME':'CELL'})

    STATIC = read_parquet_s3(_s3_api, _filename_static_tmp, BUCKET)
    STATIC = STATIC.set_index('CO_GID')
    STATIC = STATIC.rename(columns={'CO_NAME':'CELL'})

    COUNTERS = COUNTERS.join(STATIC)
    COUNTERS['TECH'] = COUNTERS['TECH'].str.upper()    
       
    ######    4G COUNTERS
    COUNTERS_4G = COUNTERS[COUNTERS.TECH == '4G'].copy()
    COUNTERS_4G['ENODEID'] = COUNTERS_4G.index
    COUNTERS_4G = COUNTERS_4G[ ['SITE','CELL','TECH','PERIOD_START_TIME','ENODEID'] + [x for x in COUNTERS_4G.columns if x in  RAW_COUNTERS_4G_COLS]]
    COUNTERS_4G = COUNTERS_4G[ pd.notnull(COUNTERS_4G['CELL'])]
    COUNTERS_4G = COUNTERS_4G.groupby(['SITE', 'CELL', 'TECH','ENODEID','PERIOD_START_TIME']).max()

    COUNTERS_4G['TIME'] = COUNTERS_4G[[
        'IP_TPUT_TIME_DL_QCI_1',
        'IP_TPUT_TIME_DL_QCI_2',
        'IP_TPUT_TIME_DL_QCI_3',
        'IP_TPUT_TIME_DL_QCI_4',
        'IP_TPUT_TIME_DL_QCI_5',
        'IP_TPUT_TIME_DL_QCI_6',
        'IP_TPUT_TIME_DL_QCI_7',
        'IP_TPUT_TIME_DL_QCI_8',
        'IP_TPUT_TIME_DL_QCI_9'
    ]].sum(axis=1)

    COUNTERS_4G['VOL'] = COUNTERS_4G[[
        'IP_TPUT_VOL_DL_QCI_1',
        'IP_TPUT_VOL_DL_QCI_2',
        'IP_TPUT_VOL_DL_QCI_3',
        'IP_TPUT_VOL_DL_QCI_4',
        'IP_TPUT_VOL_DL_QCI_5',
        'IP_TPUT_VOL_DL_QCI_6',
        'IP_TPUT_VOL_DL_QCI_7',
        'IP_TPUT_VOL_DL_QCI_8',
        'IP_TPUT_VOL_DL_QCI_9'
    ]].sum(axis=1)/1000000   #Mbits

    COUNTERS_4G['THRPUT'] = COUNTERS_4G['VOL']/3600
    COUNTERS_4G['CCUSERS'] = COUNTERS_4G.RRC_CONNECTED_UE_MAX.fillna(0)
    COUNTERS_4G['USER_THRPUT'] = COUNTERS_4G.apply(lambda x: x['THRPUT'] / x['CCUSERS'] if x['CCUSERS'] > 0 else pd.NA, axis=1) #Mbits/s
    COUNTERS_4G.reset_index(inplace=True)

    COUNTERS_4G = COUNTERS_4G[[
        'SITE',
        'CELL',
        'TECH',
        'ENODEID',
        'PERIOD_START_TIME',
        'VOL',
        'THRPUT',
        'CCUSERS',
        'USER_THRPUT'
    ]]
    
    
    #####################################
    ##################################
    ##### 3G
    COUNTERS_3G = COUNTERS[COUNTERS.TECH == '3G'].copy()
    COUNTERS_3G['ENODEID'] = COUNTERS_3G.index
    COUNTERS_3G = COUNTERS_3G[ ['SITE','CELL','TECH','PERIOD_START_TIME','ENODEID'] + [x for x in COUNTERS_3G.columns if x in  RAW_COUNTERS_3G_COLS]]
    COUNTERS_3G = COUNTERS_3G[ pd.notnull(COUNTERS_3G['CELL'])]
    COUNTERS_3G = COUNTERS_3G.groupby(['SITE', 'CELL', 'TECH','ENODEID','PERIOD_START_TIME']).max()

    COUNTERS_3G['VOL'] = (COUNTERS_3G.RECEIVED_HS_MACD_BITS + (COUNTERS_3G.MC_HSDPA_ORIG_DATA_PRI + COUNTERS_3G.MC_HSDPA_ORIG_DATA_SEC)*8)/1000 #Mbits
    COUNTERS_3G['THRPUT'] = COUNTERS_3G['VOL']/(3600)   #Mbit/s
    COUNTERS_3G['USER_THRPUT'] = pd.NA
    COUNTERS_3G.loc[COUNTERS_3G.HSDPA_BUFF_WITH_DATA_PER_TTI >0,'USER_THRPUT'] = ((COUNTERS_3G.HSDPA_ORIG_DATA*8*500)/COUNTERS_3G.HSDPA_BUFF_WITH_DATA_PER_TTI)/1000  #Mbits
    COUNTERS_3G['CCUSERS'] = COUNTERS_3G.MAX_HSDPA_USERS_IN_CELL

    COUNTERS_3G.reset_index(inplace=True)

    COUNTERS_3G = COUNTERS_3G[[
        'SITE',
        'CELL',
        'TECH',
        'ENODEID',
        'PERIOD_START_TIME',
        'VOL',
        'THRPUT',
        'CCUSERS',
        'USER_THRPUT'
    ]]

    ####################
    ##################
    
    
    

    COUNTERS_ALL = pd.concat([COUNTERS_3G,COUNTERS_4G])
    
    
    _file_reports = 'Nokia/Reports_60min/%s/REPORT_NOKIA_%s.parquet'%(_year_prefix,_date_prefix_by_day)
    print("saving file: %s"%_file_reports)
    save_parquet_s3(_s3_api, COUNTERS_ALL, BUCKET, _file_reports)
    ti.xcom_push(key='file_reports', value=_file_reports)
    
    return True
    
with DAG(
    dag_id='dw_netact_sql_hours_counters3',
    schedule_interval= "@hourly",
    default_args={
        "depends_on_past": False,
        'owner': 'Vmasuero',
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
    },
    start_date=pendulum.datetime( 2024, 11, 3, tz='America/Santiago'),
    catchup=False,
    tags=['development', 'netact'],
    max_active_runs=1
    ) as dag:
   
        with TaskGroup(group_id='dw_tables_counters') as tasks_dw_tables_counters:
            for i,table in enumerate(NETACT_TABLES_COUNTERS):
                
                task_dw_table = PythonOperator(
                    task_id='dw_table_%s'%table,
                    python_callable=dw_table_counters,
                    provide_context=True,
                    execution_timeout=timedelta(hours=1),
                    pool='NOKIA_CM_POOL_HOURS',
                    op_kwargs={
                        'table_name': table
                    }
                )
                
                task_dw_table
       
       
        with TaskGroup(group_id='dw_tables_statics') as tasks_dw_tables_statics:
            for i,table in enumerate(NETACT_TABLES_STATIC):
                task_dw_table = PythonOperator(
                    task_id='dw_table_%s'%table,
                    python_callable=dw_table_static,
                    provide_context=True,
                    execution_timeout=timedelta(hours=1),
                    pool='NOKIA_CM_POOL_HOURS',
                    op_kwargs={
                        'table_name': table
                    }
                )
       
       
       
       
        chain(
            get_dates(),
            tasks_dw_tables_counters,
            tasks_dw_tables_statics,
            create_table_static(),
            create_table_counters(),
            create_diary_report()
        )
        

    
if __name__ == "__main__":
    dag.cli()
