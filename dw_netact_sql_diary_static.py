import pendulum
import pandas as pd
import oracledb
from datetime import datetime, timedelta
import os
import boto3
from io import StringIO,BytesIO


from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowException, AirflowSkipException


NETACT_TABLES_STATIC = [
    'UTP_COMMON_OBJECTS',
    'C_LTE_LNCEL_FDD',
    'C_LTE_LNBTS',
    'OBJREG_REGISTRY_OBJECT'
]


NETACT_IPS = ['172.16.42.133', '172.16.52.133']
DB_DIR = 'netact/raw_tables_diary_static'

SECRET_KEY ='2DhT3mGRLmNDBOl9ZuxCLdic0jXSmfUiZ+niJrwp3cU='
ACCESS_KEY = 'd7556c3cc7c1996477a5c851b51e2f47ea4d00a6'
REGION = 'sa-santiago-1'
NAMESPACE = 'axosppplfddw'
BUCKET = 'bucket-scl-prod-monitoreosscc-datalake-001'
ENDPOINT = "https://%s.compat.objectstorage.%s.oraclecloud.com"%(NAMESPACE,REGION)
#bucket_url = f'https://objectstorage.{REGION}.oraclecloud.com/n/{NAMESPACE}/b/{BUCKET}/o/arieso/tmp/file_arieso.csv'

def sa_csv_s3(s3,df, BUCKET, path):
        buffer = BytesIO()
        df.to_csv(buffer, compression = "gzip", sep=";", index=False)
        buffer.seek(0)
        s3.put_object(Bucket=BUCKET, Key=path, Body=buffer.getvalue())

def sa_parquet_s3(s3,df, BUCKET, path):
        buffer = BytesIO()
        df.to_parquet(buffer, engine='pyarrow')
        buffer.seek(0)
        s3.put_object(Bucket=BUCKET, Key=path, Body=buffer.getvalue())
    
def read_file_csv(s3,BUCKET,path,sep,usecols):
    try:
        response = s3.get_object(Bucket=BUCKET, Key=path)
        _df = pd.read_csv(BytesIO(response['Body'].read()),compression='gzip',sep= ';')
     
    except pd.errors.EmptyDataError:
        print("Error: El archivo CSV está vacío.")
        return pd.DataFrame()
    except pd.errors.ParserError as e:
        print(f"Error al analizar el archivo CSV: {e}")
        return pd.DataFrame()
    except s3.exceptions.NoSuchKey:
        print("Error: El archivo no existe en el bucket S3.")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error desconocido: {e}")
        return pd.DataFrame()
    return(_df)
        
def get_dates(data_interval_end=None,data_interval_start=None, ds=None, ti=None,  **kwargs):
    from copy import copy

    print("Date in RAW version: %s "%ds)
    _date = datetime.strptime(ds, '%Y-%m-%d')
    _date_str = _date.strftime('%Y-%m-%d')
    _date_str_output = (_date + timedelta(days=1)).strftime('%Y-%m-%d')
    
    _date_ini = (_date - timedelta(days=0)).replace(hour=0, minute=0, second=0).strftime('%m-%d-%Y %H:%M:%S')
    _date_end = (_date - timedelta(days=0)).replace(hour=23, minute=59, second=0).strftime('%m-%d-%Y %H:%M:%S')

    print("Init date: %s"%_date_ini)
    print("End date: %s"%_date_end)
    
    
     
    _input_static_file = f"netact/raw_tables_diary_static/{_date_str}/UTP_COMMON_OBJECTS_{_date_str}.csv.gz"
    _output_static_file = f"netact/raw_tables_diary_static/{_date_str_output}/static_{_date_str_output}_file.parquet"
     
    ti.xcom_push(key='Date initial', value=_date_ini)
    ti.xcom_push(key='Date End', value=_date_end)
    ti.xcom_push(key='_output_static_file', value=_output_static_file)
    ti.xcom_push(key='_input_static_file', value=_input_static_file)
    
    return True

def get_filename(table:str, prefix:str, **kwargs):
    _directory = '%s/%s'%(DB_DIR,prefix)
    _output_file = "%s/%s_%s.csv.gz"%(_directory,table,prefix)

    return _output_file

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

def dw_table_static(table_name:str, ds=None, ti=None,  **kwargs):
    
    _s3 = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION,
        endpoint_url=ENDPOINT
    )
    
    print("Extarcting info from table: %s"%table_name)
    _sql_query="SELECT * FROM %s"%table_name

    _filename = get_filename(table_name, ds)
    if os.path.exists(_filename):
        print("File already exists: %s"%_filename)
        return True

    print("Exceuting: %s"%_sql_query)
    _table = get_table(table_name, _sql_query)

    if _table.empty:
        raise AirflowException

    print("Saving Table %s in file %s "%(table_name,_filename) )
    sa_csv_s3(_s3,_table, BUCKET, _filename)
    ti.xcom_push(key=table_name, value=_filename)

    return True 

def Proceso_carga(ti = None,**kwargs):
    
    _s3 = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION,
        endpoint_url=ENDPOINT
    )
    
    
    def proc_co_dn(line:str):
        _data = line.split('/')
        _ret  = {x.split('-')[0]:x.split('-')[1] for x in _data[1:]}
        return _ret
        
    _path_input = ti.xcom_pull(task_ids='get_dates', key='_input_static_file')
    print("aki",_path_input)
    parametro_path = ti.xcom_pull(task_ids='get_dates', key='_output_static_file')
    
    _idx_columns = ['CO_GID','CO_DN','CO_NAME']
    _utp_common_df = read_file_csv(_s3,BUCKET,_path_input,";",_idx_columns)
    
    
    _utp_common_df = _utp_common_df[pd.notnull(_utp_common_df['CO_DN'])]
    _utp_common_df = _utp_common_df[pd.notnull(_utp_common_df['CO_NAME'])]
    _utp_common_df = _utp_common_df[_idx_columns]
    
    _parmams_df = pd.DataFrame([ pd.Series(proc_co_dn(it[1]), name=it[0]) for it in _utp_common_df.CO_DN.items() ] )
    _utp_common_df = _utp_common_df.join(_parmams_df)
    
    _utp_common_df.loc[ pd.notnull(_utp_common_df.NRBTS),'TECH'] = '5G'
    _utp_common_df.loc[ pd.isnull(_utp_common_df.NRBTS) & pd.notnull(_utp_common_df.MRBTS),'TECH'] = '4G'
    _utp_common_df.loc[ pd.notnull(_utp_common_df.RNC),'TECH'] = '3G' 
    _utp_common_df.loc[ pd.notnull(_utp_common_df.BSC),'TECH'] = '2G'
    
    _non_identified_tech = _utp_common_df[pd.isnull(_utp_common_df.TECH)]
    print('%s cells  not identify Tech'%len(_non_identified_tech) )
    _utp_common_df = _utp_common_df[pd.notnull(_utp_common_df.TECH)]
    _utp_common_df.reset_index(inplace=True)
    _utp_common_df = _utp_common_df.drop('index', axis=1)
    
    print("Indentifcando nombres de las celdas")
    _names0 = _utp_common_df[_utp_common_df.CO_NAME.str.match('(\d+)([ispISP])?.*(\d\d\d)[:-]?(\d+)?')]
    _names0 = _names0.CO_NAME.str.extract('(\d+)([ispISP])?.*(\d\d\d)[:-]?(\d+)?')
    _names0[3] = _names0[3].fillna(0)
    _names0[1] = _names0[1].fillna('').str.lower()
    _names0 = _names0.astype(str).apply(lambda x: x[0]+x[1]+'_'+x[2], axis=1)
    _names0 = pd.DataFrame({'SITE':_names0})
    
    _names1 = _utp_common_df[_utp_common_df.CO_NAME.str.match('([A-Z]+)(\d+)?[A-Z_](\d\d\d)[-:]?(\d+)?')]
    _names1 = _names1.CO_NAME.str.extract('([A-Z]+)(\d+)?[A-Z_](\d\d\d)[-:]?(\d+)?')
    _names1[3] = _names1[3].fillna(0)
    _names1[1] = _names1[1].fillna('').str.lower()
    _names1 = _names1.astype(str).apply(lambda x: x[0]+x[1]+'_'+x[2], axis=1)
    _names1 = pd.DataFrame({'SITE':_names1})
    
    _names = pd.concat([_names0, _names1])
    _utp_common_df = _utp_common_df.join(_names)
    _non_identified_sites = _utp_common_df[pd.isnull(_utp_common_df.SITE)]
    
    _utp_common_df = _utp_common_df[pd.notnull(_utp_common_df['SITE'])]
    
    sa_parquet_s3(_s3,_utp_common_df, BUCKET, parametro_path)
    
    
with DAG(
    dag_id='dw_netact_sql_diary_static',
    schedule_interval= "@daily",
    default_args={
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    start_date=pendulum.datetime( 2024, 8, 24, tz='America/Santiago'),
    catchup=False,
    tags=['development', 'netact'],
    max_active_runs=1
) as dag:
   
    task_get_dates = PythonOperator(
        task_id='get_dates',
        python_callable=get_dates,
        provide_context=True,
        op_kwargs={}
    )

    with TaskGroup(group_id='dw_tables_statics') as tasks_dw_tables_statics:

        for i,table in enumerate(NETACT_TABLES_STATIC):
            task_dw_table = PythonOperator(
                task_id='dw_table_%s'%table,
                python_callable=dw_table_static,
                provide_context=True,
                execution_timeout=timedelta(hours=2),
                op_kwargs={
                    'table_name': table
                }
            )



    Proceso_carga = PythonOperator(
        task_id='Proceso_carga',
        python_callable=Proceso_carga,
        provide_context=True,
        op_kwargs={}
    )
    
    
    task_get_dates >> tasks_dw_tables_statics >> Proceso_carga
    
if __name__ == "__main__":
    dag.cli()