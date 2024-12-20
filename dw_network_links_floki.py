from datetime import datetime, timedelta
import pendulum
import requests
import pandas as pd
import urllib.parse


from airflow import DAG
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException, AirflowFailException, AirflowSkipException

URL_API = 'http://200.27.26.27/cgi-bin/reporte_diario.pl'
#http://200.27.26.27/cgi-bin/reporte_diario.pl?ano=2023&mes=12&dia=23&tab=CORE%20INT.


PROXY_PARAMS = {
    "http":"http://10.36.13.147:3128"
}

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
    'RED MPLS CORPORACIONES'
]

S3_PATH = 'NETWORK_COUNTERS/FLOKI'


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
    ti.xcom_push(key='Date Prefix', value=_date_prefix)
    
    return True
 
@task(
    executor_config={'LocalExecutor': {}},
)
def dw_files(tab_floki:str, ti=None,  **kwargs):


    _url_floki = ti.xcom_pull(task_ids="get_dates", key="url_floki")
    _output_directory = ti.xcom_pull(task_ids="get_dates", key="output_directory")
    
    _url = "%s&tab=%s"%(
        _url_floki,
        urllib.parse.quote(tab_floki)

    )
    print("URL: %s"%_url)

    _req = requests.get(_url, proxies=PROXY_PARAMS)

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

    
    
    #print("Writing content in file: %s"%_filename)
    #open(_filename, 'wb').write(_req.content)


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
            dw_files(tab_floki=tab)
   
    initialization() >> tasks_dw_floki