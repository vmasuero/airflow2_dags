from airflow import DAG
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException, AirflowFailException, AirflowSkipException

from datetime import datetime, timedelta
import pendulum
import boto3
import pandas as pd

S3_PATH = 'NETWORK_COUNTERS/OYM'
S3_PATH_HEADERS = 'NETWORK_COUNTERS/HEADERS'

SECRET_KEY ='2DhT3mGRLmNDBOl9ZuxCLdic0jXSmfUiZ+niJrwp3cU='
ACCESS_KEY = 'd7556c3cc7c1996477a5c851b51e2f47ea4d00a6'
REGION = 'sa-santiago-1'
NAMESPACE = 'axosppplfddw'
BUCKET = 'bucket-scl-prod-monitoreosscc-datalake-001'
ENDPOINT = "https://%s.compat.objectstorage.%s.oraclecloud.com"%(NAMESPACE,REGION)
bucket_url = f'https://objectstorage.{REGION}.oraclecloud.com/n/{NAMESPACE}/b/{BUCKET}/o/arieso/tmp/file_arieso.csv'

def read_parquet_from_s3(path:str, s3_api):
    obj_buffer = s3_api.Object(BUCKET, path)
    
    with BytesIO(obj_buffer.get()['Body'].read()) as buffer:
        _df = pd.read_parquet(buffer)
        
    _df.reset_index(inplace=True)
    
    return _df
    
def read_csv_from_s3(path:str, s3_api):
    obj_buffer = s3_api.Object(BUCKET, path)
    
    with BytesIO(obj_buffer.get()['Body'].read()) as buffer:
        _df = pd.read_csv(buffer, sep=';')
        
    _df.reset_index(inplace=True)
    
    return _df
    
def file_exists(bucket_name, key):
    
    _s3 = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION,
        endpoint_url=ENDPOINT
    )

    try:
        _s3.head_object(Bucket=bucket_name, Key=key)
        return True
    except Exception as e:
        if e.response['Error']['Code'] == "404":
            print('File not found: 404')
            return False
    
    return False


def get_list_files(bucket_name, path:str):


    _list = []
    
    _s3 = boto3.resource(
        's3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION,
        endpoint_url=ENDPOINT
    )
    
    _list = [x.key for x in _s3.Bucket(bucket_name).objects.filter(Prefix=path)]

    return _list






@task(
    executor_config={'LocalExecutor': {}},
)
def initialization(yesterday_ds = None, ds=None, ti=None, ds_nodash=None,  **kwargs):
    
    print("Yesteraday Date in RAW version: %s "%yesterday_ds)
    _date = datetime.strptime(str(yesterday_ds), "%Y-%m-%d")
    
    _year = _date.year
    _output_dir = "%s/%s"%(S3_PATH,_year)
    
    _file_traffic = '%s_ClaroVtr_Traffic_v2.parquet'%(ds_nodash)
    _file_devifs = '%s_ClaroVtr_Devifs.parquet'%(ds_nodash)
    
    
    _file_s3_traffic = "%s/%s"%(_output_dir, _file_traffic)
    _file_s3_devifs = "%s/%s"%(_output_dir, _file_devifs)
    
    _file_s3_headers =  get_list_files(BUCKET, S3_PATH_HEADERS)
    _file_s3_headers = [x for x in _file_s3_headers if 'network_headers_v' in x]
    
    _last_header = sorted(_file_s3_headers)[-1].split('/')[-1].split('.')[0]
    _last_header = "%s/%s"%(S3_PATH_HEADERS, _last_header)

    
    
    
    
    if not file_exists(BUCKET, _file_s3_traffic):
        raise AirflowFailException("file not in S3: %s"%_file_s3_traffic)
    else:
        print("File %s exist en S3"%_file_s3_traffic)
    
    
    ti.xcom_push(key='output_dir', value=_output_dir)
    ti.xcom_push(key='file_s3_traffic', value=_file_s3_traffic)
    ti.xcom_push(key='file_s3_devifs', value=_file_s3_devifs)
    ti.xcom_push(key='last_header', value=_last_header)
    
    return True
 




@task(
    executor_config={'LocalExecutor': {}}
)
def proc_header_file(ti=None):

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
            'Pta B',
            'Capacidad'
    ]


    _header_file = ti.xcom_pull(task_ids='initialization', key='last_header')
    print('Reading Header File: %s'%_header_file)
    
    _headers = read_csv_from_s3(_header_file)
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
    #_headers['Pta B'] = _headers['Pta B'].str.lower().str.replace("'","").fillna('unknow')
    _headers['Localidad B'] = _headers['Localidad B'].astype(str).fillna('unknow').str.strip()
    
    _headers.loc[ _headers['Localidad A'] == 'nan', 'Localidad A'] = 'unknow'
    _headers.loc[ _headers['Localidad B'] == 'nan', 'Localidad B'] = 'unknow'
    
    _headers['Descripcion'] = _headers['Descripcion'].astype(str)
    _headers['Capacidad'] = _headers['Capacidad'].astype(float)


    _headers['device_hash'] = _headers['Extremo A'].apply(format_name)
    _headers['port_hash'] = _headers['Pta A'].apply(format_port)
    
    _headers[_headers.select_dtypes('object').columns] = _headers[_headers.select_dtypes('object').columns].apply(lambda x: x.str.strip())

    _headers['hash'] = _headers.apply(lambda x: create_hash(x['device_hash'],x['port_hash']), axis=1)
    
    _headers = _headers[HEADERS_COLS + ['hash']]

    ti.xcom_push(key='headers', value=_headers.to_dict('records'))
    
    return True

 
with DAG(
    dag_id ='report_diary_bw5',
    schedule_interval = "30 12 * * *",
    default_args = {
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
        "provide_context": True
    },
    start_date = pendulum.datetime( 2024, 6, 1, tz='America/Santiago'),
    catchup = False,
    max_active_runs=3,
    tags=['reports','bw']
) as dag:

    initialization() >> proc_header_file()


