from airflow import DAG

from airflow.utils.decorators import apply_defaults
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
#from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

from datetime import datetime, timezone, timedelta

import psycopg2

from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient

import clickhouse_connect
from clickhouse_driver import Client
from clickhouse_driver.errors import Error

import re

import sys
sys.path.append('/usr/lib/ViaviAnalytics')
import Nsa5GAnalyticsStreamingDataFeed_pb2

KAFKA_EPS = [
    {'name':'clarochilelldr01-kafka', 'ip':'172.18.90.72'},
    {'name':'clarochilelldr02-kafka', 'ip':'172.18.90.74'},
    {'name':'clarochilelldr03-kafka', 'ip':'172.18.90.76'},
    {'name':'clarochilelldr04-kafka', 'ip':'172.18.90.80'},
    {'name':'clarochilelldr05-kafka', 'ip':'172.18.90.82'},
    {'name':'clarochilelldr06-kafka', 'ip':'172.18.90.84'},
    {'name':'clarochilelldr07-kafka', 'ip':'172.18.90.86'},
    {'name':'clarochilelldr08-kafka', 'ip':'172.18.90.88'},
    {'name':'clarochilelldr09-kafka', 'ip':'172.18.90.90'},
    {'name':'clarochilelldr10-kafka', 'ip':'172.18.90.36'},
    {'name':'clarochilelldr11-kafka', 'ip':'172.18.90.38'},
    {'name':'clarochilelldr12-kafka', 'ip':'172.18.90.40'},
    {'name':'clarochilelldr13-kafka', 'ip':'172.18.90.42'},
    {'name':'clarochilelldr14-kafka', 'ip':'172.18.90.44'}
    
]
KAFKA_TOPIC = 'Nsa5GAnalyticsStreamingFeedRecords'
KAFKA_GROUPID = 'AirflowClickHouse'
KAFKA_NAMES = [x['name'] for x in KAFKA_EPS]


CLICKHOUSE_IP = 'clickhouse-counters-sscc.clickhouse.svc.cluster.local' 
SHARDS = [x for x in range(5)]
CLICKHOUSE_IP_SHARDS = ['chi-counters-sscc-counters-sscc-%s-0.clickhouse.svc.cluster.local'%x for x in SHARDS]

CLUSTER = 'counters-sscc'
CLICKHOUSE_PORT = 8123      
CLICKHOUSE_USERNAME = 'dev_user' 
CLICKHOUSE_PASSWORD = 'vtrclaro1234'      
DATABASE = 'VIAVI' 

TABLE='FEEDS_NR'
TABLE_DIST = "%s_DIST"%TABLE


DECO_NSA = Nsa5GAnalyticsStreamingDataFeed_pb2.Nsa5GAnalyticsStreamingFeedRecord()

MAX_MESSAGES_RECEIVED = 1000000
MAX_DURATION_MS = 50000000

PRI_FIELDS = [
    'SegmentStartTime',
    'SegmentEndTime',
    'Imsi',
    'NrCells',
    'LteStartCellName'
]

REQUIRED_COLS = [
    'segmentstarttime',
    'imsi',
    'ltestartcellname',
    'minutesofuse',
    'nrcells_nrcelllabel',
    'nrcells_medianaveragersrp',
    'nrcells_durationms',
    'nrerab_nruplinkvolumebytes',
    'nrerab_nrdownlinkvolumebytes',
    'nrerab_nraverageuplinkthroughput',
    'nrerab_nraveragedownlinkthroughput',
    'nrerab_lteaverageuplinkthroughput',
    'nrerab_lteaveragedownlinkthroughput',
    'nrerab_overallaverageuplinkthroughput',
    'nrerab_overallaveragedownlinkthroughput'
]

PRI_FIELDS = [x.lower() for x in PRI_FIELDS]

def conv_dict_sql(data:dict, table:str, database:str):
    table_name = table
    columns = ", ".join(data.keys())
    values = ", ".join(f"'{v}'" if isinstance(v, str) else str(v) for v in data.values())
    
    return  f"INSERT INTO {database}.{table_name} ({columns}) VALUES ({values});"
    

@task(
    executor_config={'LocalExecutor': {}},
    pool='KAFKA_FEEDERS'
)
def receivers(topic, kafka_config, broker_id, max_messages, **kwargs):

    

    def process_message(msg_obj, broker_id:str):

        ret_dicts = []
        
        ret_dict = {}
        ret_header = {}
        
        _exist_fields = [x[0].name.lower() for x in msg_obj.ListFields()]
           
        if not all([x in _exist_fields for x in PRI_FIELDS]):
            print("Missing:")
            print([x  for x in PRI_FIELDS if x not in _exist_fields])
            return {}

        if not re.match(r'^\d\d.*', msg_obj.LteStartCellName.lower()):
            return {}
        
        if not re.match(r'^\d+.*', msg_obj.Imsi.lower()):
            return {}
            


        ret_header['SegmentStartTime'] = msg_obj.SegmentStartTime
        ret_header['SegmentEndTime'] = msg_obj.SegmentEndTime
        ret_header['Imsi'] = msg_obj.Imsi
        ret_header['LteStartCellName'] = msg_obj.LteStartCellName
        ret_header['MinutesOfUse'] = msg_obj.MinutesOfUse
        
        ret_header['Longitude'] = msg_obj.Longitude
        ret_header['Latitude'] = msg_obj.Latitude

        for i,obj_nrcell in enumerate(msg_obj.NrCells):
            dict_append = ret_header.copy()
            dict_append['nrcells_nrcelllabel'] = obj_nrcell.NrCellLabel
            dict_append['nrcells_medianaveragersrp'] = obj_nrcell.MedianAverageRsrp
            dict_append['nrcells_durationms'] = obj_nrcell.DurationMs
            
            if obj_nrcell.DurationMs > MAX_DURATION_MS:
                print('Max Time Duration Ms')
                return {}

            
            if ('nrerab' in _exist_fields) & (i == 0):
                dict_append['nrerab_NrUplinkVolumeBytes'] = msg_obj.NrErab[0].NrUplinkVolumeBytes
                dict_append['nrerab_NrDownlinkVolumeBytes'] = msg_obj.NrErab[0].NrDownlinkVolumeBytes
                dict_append['nrerab_NrAverageUplinkThroughput'] = msg_obj.NrErab[0].NrAverageUplinkThroughput
                dict_append['nrerab_NrAverageDownlinkThroughput'] = msg_obj.NrErab[0].NrAverageDownlinkThroughput
                dict_append['nrerab_LteAverageUplinkThroughput'] = msg_obj.NrErab[0].LteAverageUplinkThroughput
                dict_append['nrerab_LteAverageDownlinkThroughput'] = msg_obj.NrErab[0].LteAverageDownlinkThroughput
                dict_append['nrerab_OverallAverageUplinkThroughput'] = msg_obj.NrErab[0].OverallAverageUplinkThroughput
                dict_append['nrerab_OverallAverageDownlinkThroughput'] = msg_obj.NrErab[0].OverallAverageDownlinkThroughput
            else:
                dict_append['nrerab_NrUplinkVolumeBytes'] = 0
                dict_append['nrerab_NrDownlinkVolumeBytes'] = 0
                dict_append['nrerab_NrAverageUplinkThroughput'] = 0
                dict_append['nrerab_NrAverageDownlinkThroughput'] = 0
                dict_append['nrerab_LteAverageUplinkThroughput'] = 0
                dict_append['nrerab_LteAverageDownlinkThroughput'] = 0
                dict_append['nrerab_OverallAverageUplinkThroughput'] = 0
                dict_append['nrerab_OverallAverageDownlinkThroughput'] = 0

            dict_append = {key.lower(): value for key, value in dict_append.items() if key.lower() in REQUIRED_COLS}
            dict_append['date_starttime']  = datetime.fromtimestamp(dict_append['segmentstarttime'] / 1000.0, tz=timezone.utc)
            dict_append['date_starttime']  = dict_append['date_starttime'].strftime("%Y-%m-%d %H:%M:%S%z")
            dict_append['id'] = dict_append['nrcells_nrcelllabel'] + '-' + str(dict_append['imsi']) + '-' + str(dict_append['segmentstarttime'])
            dict_append['broker_id'] = broker_id
            del dict_append['segmentstarttime']

            
            ret_dicts.append(dict_append)

        return ret_dicts

    KAFKA_CONSUMER = Consumer(kafka_config)
    KAFKA_CONSUMER.subscribe([topic])
    
    CLIENT_CH = clickhouse_connect.get_client(
        host= CLICKHOUSE_IP, 
        username= CLICKHOUSE_USERNAME, 
        password= CLICKHOUSE_PASSWORD
    )
    
    message_count = 0
        
    while message_count < max_messages:
            
        msg = KAFKA_CONSUMER.poll(timeout=5)  #5 segundos
                
        if msg is None:
            break
            
        if msg.error():
            print("Consumer error: %s", msg.error())
            continue
                    
        msg_obj = DECO_NSA.FromString(msg.value())
        msgs_received = process_message(msg_obj, broker_id)
        
        if msgs_received == []:
            print(msg.value())
            print('No data to upload')
            continue
        
        


        try:
        
            for msg_received in msgs_received:
                _sql_row = conv_dict_sql(msg_received, TABLE_DIST, DATABASE)
                CLIENT_CH.command(_sql_row)
                
        except Error as ch_err:
            print(ch_err)
            print(msg.value())
            continue
            
        message_count += 1

    if message_count >= max_messages:
        print("MAX REACHED, Processed maximum number of messages: %s"%max_messages)
    elif message_count > 0:
        print("Processed  messages: %s"%message_count)
    else:
        print('No info received')


    return True

@task(
    executor_config={'LocalExecutor': {}},
)
def initialization(ds=None, ti=None, **kwargs):

    print("TOPIC: %s"%KAFKA_TOPIC)
    print(ds)
    
    print('Chequing Postgress: %s'%CLICKHOUSE_IP)
    CLIENT_CH = clickhouse_connect.get_client(
        host= CLICKHOUSE_IP, 
        username= CLICKHOUSE_USERNAME, 
        password= CLICKHOUSE_PASSWORD
    )
    
    try:
        result = CLIENT_CH.command('SELECT 1')
        print("ClickHouse is working:", result)
    except Error as e:
        print("ClickHouse is not available:", e)
        
    return True
    

with DAG(
    dag_id='stream_arieso_clickhouse',
    default_args={
        "depends_on_past": False,
        'owner': 'Vmasuero'
    },
    schedule_interval='*/15 * * * *',
    start_date=days_ago(1),
    max_active_runs= 1,
    dagrun_timeout=timedelta(minutes=30),
    tags=['development', 'arieso', 'kafka'],
    catchup=False
    ) as dag:
    
        with TaskGroup(group_id='consumers_tasks') as consumers_tasks:

            for i,broker_id in enumerate(KAFKA_NAMES[:]):
                print("adding broker: %s"%broker_id)
                
                _kafka_config={
                    "bootstrap.servers": "%s:9092"%broker_id, 
                    "group.id": KAFKA_GROUPID,
                    "auto.offset.reset": "earliest",
                }
                receivers(KAFKA_TOPIC, _kafka_config, broker_id, MAX_MESSAGES_RECEIVED)
                
                
        initialization() >> consumers_tasks
        
if __name__ == "__main__":
    dag.cli()
