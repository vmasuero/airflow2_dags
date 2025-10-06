from datetime import datetime, timedelta
import pendulum
import requests
import pandas as pd
import urllib.parse
import boto3
from io import StringIO,BytesIO

from airflow import DAG
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException, AirflowFailException, AirflowSkipException
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

URL_API = 'http://200.27.26.27/cgi-bin/reporte_diario.pl'
URL_LIST_ELEMS = 'http://200.27.26.27/cgi-bin/listaElementos.pl'
#http://200.27.26.27/cgi-bin/reporte_diario.pl?ano=2023&mes=12&dia=23&tab=CORE%20INT.


PROXY_PARAMS = {
    "http":"http://10.36.13.147:3128",
    "https":"http://10.36.13.147:3128",

    
}

FLOKI_TEST_PAGE = "http://200.27.26.27/cgi-bin/reporte_diario.pl?ano=2025&mes=05&dia=19&tab=IPRAN"


FILTER_TAB = [
    'IPRAN',
    #'CORE INT.',
    #'PEERING NAC',
    #'SOLOP',
    #'HFC', 
    #'RED HFC CMTS',
    #'HFC FTTH',
    #'CACHE',
    #'ISP PEERING CONTENIDO',
    #'ISP-CACHE-CONTENIDO',
    #'PEERING INT',
    #'SW-HFC-ACCESO',
    #'MOVIL-PE',
    #'SWITCH CORP - PLAN',
    #'RED MPLS CORPORACIONES'
    #'CUSTOMER CORP'
]

S3_PATH = 'NETWORK_COUNTERS/FLOKI'
S3_PATH_LINKS = 'NETWORK_COUNTERS/FLOKI_LINKS'

SECRET_KEY ='2DhT3mGRLmNDBOl9ZuxCLdic0jXSmfUiZ+niJrwp3cU='
ACCESS_KEY = 'd7556c3cc7c1996477a5c851b51e2f47ea4d00a6'
REGION = 'sa-santiago-1'
NAMESPACE = 'axosppplfddw'
BUCKET = 'bucket-scl-prod-monitoreosscc-datalake-001'
ENDPOINT = "https://%s.compat.objectstorage.%s.oraclecloud.com"%(NAMESPACE,REGION)
bucket_url = f'https://objectstorage.{REGION}.oraclecloud.com/n/{NAMESPACE}/b/{BUCKET}/o/arieso/tmp/file_arieso.csv'



@task(
    executor_config={'LocalExecutor': {}},
)
def initialization(data_interval_start=None, ti=None, ds=None,  **kwargs):
    
    
    #2023-10-18T18:00:00+00:00
    _time_start = pd.to_datetime(data_interval_start)

    _output_directory = "%s/%s"%(
        S3_PATH,
        str(_time_start.year)
    )

    _output_file_elements = "%s/%s/floki_elements_%s%s%s.csv"%(
        S3_PATH_LINKS,
        str(_time_start.year),
        str(_time_start.year),
        str(_time_start.month).zfill(2),
        str(_time_start.day).zfill(2)
    )


    _url_floki = "%s?ano=%s&mes=%s&dia=%s"%(
        URL_API,
        str(_time_start.year),
        str(_time_start.month).zfill(2),
        str(_time_start.day).zfill(2),
    )

    _date_prefix = "_%s-%s-%s"%(
        str(_time_start.year),
        str(_time_start.month).zfill(2),
        str(_time_start.day).zfill(2),
    )


    print("Date in RAW version: %s "%ds)
    print("Date in START INTERVAL version: %s "%_time_start)

    ti.xcom_push(key='url_floki', value=_url_floki)
    ti.xcom_push(key='output_directory', value=_output_directory)
    ti.xcom_push(key='output_file_elements', value=_output_file_elements)
    ti.xcom_push(key='Date Prefix', value=_date_prefix)
    
    return True
 
 

@task(
    executor_config={'LocalExecutor': {}},
)
def test_proxy(**kwargs):
    
    proxy_server = Variable.get("PROXY_CORP")
    
    proxy = {
        "http": proxy_server,
        "https": proxy_server
    }
    

    
    try:
        response = requests.get(URL_API, proxies=PROXY_PARAMS)
        print(f"Testing: {URL_API}")
        print(response.status_code )
        print(response.headers )
        print(response)
    
    except requests.exceptions.RequestException as e:
        print("Proxy failed:", e)
        raise AirflowFailException('Fail form Server Floki')
    
    return True
    
    

@task(
    executor_config={'LocalExecutor': {}},
)
def dw_files_upload_s3(tab_floki:str, ti=None,  **kwargs):


    _url_floki = ti.xcom_pull(task_ids="initialization", key="url_floki")
    _output_directory = ti.xcom_pull(task_ids="initialization", key="output_directory")
    
    
    _s3_api = boto3.resource('s3',
        aws_access_key_id = ACCESS_KEY,
        aws_secret_access_key = SECRET_KEY,
        region_name = REGION, 
        endpoint_url = ENDPOINT 
    )
    
    _url = "%s&tab=%s"%(
        _url_floki,
        urllib.parse.quote(tab_floki)

    )
    print("URL: %s"%_url)

    _req = requests.get(_url, proxies=PROXY_PARAMS)
    _req.raise_for_status()

    if _req.status_code != 200:
        raise AirflowFailException("Codigo de error recibido %s"%str(_req.status_code))

    _headers = _req.headers

    if 'application/x-download' not in _headers['Content-Type']:
        raise AirflowFailException("Tipo datos desconocid %s"%_headers['Content-Type'])

    if 'filename' not in _headers['Content-Disposition']:
        raise AirflowFailException("Cotenido desconocido %s"%_headers['Content-Disposition'])
    
    _filename = _headers['Content-Disposition'].replace("\"","").split('filename=')[1]
    print(_headers)

    _filename = "%s/%s"%(
        _output_directory,
        _filename
        )
    
    ti.xcom_push(key='filename', value=_filename)
    print("Writing content in file: %s"%_filename)

    _s3_api.Bucket(BUCKET).put_object(
        Key=_filename,
        Body=BytesIO(_req.content),
        ContentType='application/gzip'  
    )
    
    return True

 
 
@task(
    executor_config={'LocalExecutor': {}},
)
def get_list_elements(ti=None, ds=None, **kwargs):


    def save_csv_s3(s3,data, bucket, path):
            
        _buffer = BytesIO()
        
        data.to_csv(_buffer, sep=";", index=False)
        _buffer.seek(0)
        
        _res = s3.Object(bucket, path).put(Body=_buffer.getvalue())
        
        return list(_res.items())[0][1]['HTTPStatusCode'] == 200

    _s3_api = boto3.resource('s3',
        aws_access_key_id = ACCESS_KEY,
        aws_secret_access_key = SECRET_KEY,
        region_name = REGION, 
        endpoint_url = ENDPOINT 
    )
    
    _output_file_elements = ti.xcom_pull(task_ids="initialization", key="output_file_elements")

    
    _req = requests.get(URL_LIST_ELEMS, proxies=PROXY_PARAMS)
    
    if _req.status_code != 200:
        raise AirflowFailException("Codigo de error recibido %s"%str(_req.status_code))
        
    _content = _req.content.decode().split('\n')
    
    if len(_content) < 10:
        print('Error retrieving list of files')
        print(_content[:5])
        return AirflowFailException("Content received too small")
            

    
    _headers = _content[0].split(';')
    _data = [x.split(';') for x in _content[1:] if len(x.split(';')) == 6]
    
    _df = pd.DataFrame(_data, columns=_headers)
    _df = _df[~_df.duplicated()].reset_index(drop=True)
    _df = _df[['Equipo', 'Puerta', 'Descripcion', 'Pestana']]

    print("Saving last file in: %s"%_output_file_elements)
    save_csv_s3(_s3_api, _df, BUCKET, _output_file_elements)

        
    return True
    
    
        
        
        
with DAG(
    dag_id='dw_network_links_floki',
    schedule_interval= "30 10 * * *",
    default_args={
        "depends_on_past": False,
        'owner': 'Vmasuero',
        "retries": 15,
        'max_active_runs': 1,
        "retry_delay": timedelta(minutes=30)
    },
    start_date=pendulum.datetime( 2024, 9, 1, tz='America/Santiago'),
    catchup=False,
    tags=['development', 'bw','floki']
) as dag:

    with TaskGroup(group_id='dw_tasks') as tasks_dw_floki:

        for i,tab in enumerate(FILTER_TAB):
            _tab_prefix = tab.lower().replace(' ','_').replace('.','').replace('-','_').replace("'",'') 
            dw_files_upload_s3(task_id='dw_files_'+_tab_prefix, tab_floki=tab)
   
    initialization() >> test_proxy() >> [tasks_dw_floki, get_list_elements()]
