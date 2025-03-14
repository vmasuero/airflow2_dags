from airflow import DAG
import pandas as pd

from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

from airflow.utils.dates import days_ago
import re


from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient

from sqlalchemy import create_engine
from sqlalchemy.engine import reflection
from sqlalchemy.exc import IntegrityError


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
KAFKA_GROUPID = 'Airflow'
KAFKA_NAMES = [x['name'] for x in KAFKA_EPS]


POSTGRES_IP = 'postgres-viavi.ntv'
POSTGRES_USER = 'admin'
POSTGRES_PASS = 'vtrclaro1234'
POSTGRES_DB = 'Nsa5GAnalyticsStreamingDataFeed'
POSTGRES_EP = 'postgresql://%s:%s@%s:5432/%s'%(POSTGRES_USER,POSTGRES_PASS,POSTGRES_IP,POSTGRES_DB)
POSTGRES_TABLE = 'feeds_nr'
POSTGRES_ENGINE = create_engine(POSTGRES_EP, connect_args={'options': '-c statement_timeout=900000'})


DECO_NSA = Nsa5GAnalyticsStreamingDataFeed_pb2.Nsa5GAnalyticsStreamingFeedRecord()

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

NRERAB_COLS = [x.replace('nrerab_','') for x in REQUIRED_COLS if 'nrerab' in x]

def process_message(msg_obj, broker_id:str) -> pd.DataFrame:

    INT_COLS = [
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
        
        
    
    def proc_message(obj_msg):
    
        def proc_nrcells(obj_msg):

            PRI_FIELDS = [
                'NrCellLabel',
                #'MedianAverageRsrp',
                #'DurationMs'
            ]
            PRI_FIELDS = [x.lower() for x in PRI_FIELDS]
            _exist_fields = [x[0].name.lower() for x in obj_msg.ListFields()]
        
            if not all([x in _exist_fields for x in PRI_FIELDS]):
                return {}
        
            return {
                'nrcells_NrCellLabel': obj_msg.NrCellLabel.lower(),
                'nrcells_MedianAverageRsrp': obj_msg.MedianAverageRsrp,
                'nrcells_DurationMs': obj_msg.DurationMs
            }

        def proc_nrerab(obj_msg):
                
            PRI_FIELDS = [
                'NrUplinkVolumeBytes', 
                'NrDownlinkVolumeBytes', 
                'NrAverageUplinkThroughput', 
                'NrAverageDownlinkThroughput', 
                'LteAverageUplinkThroughput',
                'LteAverageDownlinkThroughput', 
                'OverallAverageUplinkThroughput',
                'OverallAverageDownlinkThroughput'
            ]
            
            return {
                'nrerab_NrUplinkVolumeBytes': obj_msg.NrUplinkVolumeBytes,
                'nrerab_NrDownlinkVolumeBytes': obj_msg.NrDownlinkVolumeBytes,
                'nrerab_NrAverageUplinkThroughput': obj_msg.NrAverageUplinkThroughput,
                'nrerab_NrAverageDownlinkThroughput': obj_msg.NrAverageDownlinkThroughput,
                'nrerab_LteAverageUplinkThroughput': obj_msg.LteAverageUplinkThroughput,
                'nrerab_LteAverageDownlinkThroughput': obj_msg.LteAverageDownlinkThroughput,
                'nrerab_OverallAverageUplinkThroughput': obj_msg.OverallAverageUplinkThroughput,
                'nrerab_OverallAverageDownlinkThroughput': obj_msg.OverallAverageDownlinkThroughput
            }
    
        ret_obj = []
        ret_header = {}
        
        ret_nrerab_zero = {
            'nrerab_NrUplinkVolumeBytes': 0, 
            'nrerab_NrDownlinkVolumeBytes': 0, 
            'nrerab_NrAverageUplinkThroughput': 0, 
            'nrerab_NrAverageDownlinkThroughput': 0, 
            'nrerab_LteAverageUplinkThroughput': 0,
            'nrerab_LteAverageDownlinkThroughput': 0, 
            'nrerab_OverallAverageUplinkThroughput': 0,
            'nrerab_OverallAverageDownlinkThroughput': 0
        } 

        PRI_FIELDS = [
            'SegmentStartTime',
            'SegmentEndTime',
            'Imsi',
            'LteStartCellName',
            'MinutesOfUse',
            'NrCells'
        ]

        PRI_FIELDS = [x.lower() for x in PRI_FIELDS]
        
        _exist_fields = [x[0].name.lower() for x in obj_msg.ListFields()]

        if not all([x in _exist_fields for x in PRI_FIELDS]):
            print("MIssing:")
            print([x  for x in PRI_FIELDS if x not in _exist_fields])
            return {}

        #if not re.match(r'.*[45]g.*', obj_msg.LteStartCellName.lower()):
        if not re.match(r'^\d\d.*', obj_msg.LteStartCellName.lower()):
            return {}

        ret_header['SegmentStartTime'] = obj_msg.SegmentStartTime
        ret_header['SegmentEndTime'] = obj_msg.SegmentEndTime
        ret_header['Imsi'] = obj_msg.Imsi
        ret_header['LteStartCellName'] = obj_msg.LteStartCellName
        ret_header['MinutesOfUse'] = obj_msg.MinutesOfUse
        
        for i,_obj_nrcell in enumerate(obj_msg.NrCells):
            ret_nrcell = proc_nrcells(_obj_nrcell) 
            
            if ret_nrcell == {}:
                return {}
            
            if ('nrerab' in _exist_fields) & (i == 0):
                ret_nrerab = proc_nrerab(obj_msg.NrErab[0])
                ret_obj.append({**ret_header,**ret_nrcell,**ret_nrerab})
            else:
                ret_obj.append({**ret_header,**ret_nrcell,**ret_nrerab_zero})

        return ret_obj
  
    _msg_df = proc_message(msg_obj)
    _msg_df = pd.DataFrame(_msg_df)

    if _msg_df.empty:
        print('Se descarta mensaje:')
        print(msg_obj.SerializeToString())
        return pd.DataFrame()
        
    _msg_df.columns = _msg_df.columns.str.lower() 
    
    for col in [x for x in _msg_df.columns if x in INT_COLS]:
        _msg_df[col] = _msg_df[col].round(0).astype(int) 
   
    _msg_df['date_starttime'] = pd.to_datetime(_msg_df['segmentstarttime'], unit='ms', utc=True)
    _msg_df['id'] = _msg_df.apply(lambda x: str(x.nrcells_nrcelllabel) + '-' + str(x.imsi) + '-' + str(x.segmentstarttime), axis=1)
    _msg_df = _msg_df[REQUIRED_COLS + ['id','date_starttime']]
    _msg_df['broker_id'] = broker_id
    del _msg_df['segmentstarttime']
    
    return _msg_df



class ConfluentKafkaSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self, topic, kafka_config, broker_id, max_messages=1, process_message_func=None, *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.topic = topic
        self.kafka_config = kafka_config
        self.max_messages = max_messages
        self.process_message_func = process_message_func
        self.message_count = 0  
        self.broker_id = broker_id

    def poke(self, context):
        self.log.info("Polling Kafka topic: %s (processed %s/%s messages)", self.topic, self.message_count, self.max_messages)
        

        consumer = Consumer(self.kafka_config)
        consumer.subscribe([self.topic])
        
        data_collected = []
        
        try:

            while self.message_count < self.max_messages:
            
                msg = consumer.poll(timeout=5)  #5 segundos
                
                if msg is None:
                    break
                    
                if msg.error():
                    self.log.error("Consumer error: %s", msg.error())
                    continue
                    
                _msg_obj = DECO_NSA.FromString(msg.value())
                _msg_df_append = self.process_message_func(_msg_obj,self.broker_id)
                data_collected.append(_msg_df_append)
                    
                self.message_count += 1

            if self.message_count >= self.max_messages:
                self.log.info("MAX REACHED, Processed maximum number of messages: %s", self.max_messages)
            else:
                self.log.info("Processed  messages: %s", self.message_count)
                
            if len(data_collected) == 0:
                print('No info received')
                return True
            
                
            DATA_COLLECTED_DF = pd.concat(data_collected)
            print("DEBUG:")
            print(DATA_COLLECTED_DF['id'].sample(5))
            print()
            
            print('Uploading to Database: %s'%POSTGRES_IP)
            DATA_COLLECTED_DF.to_sql(POSTGRES_TABLE, POSTGRES_ENGINE, if_exists='append', index=False)     
            return True

        except KafkaException as e:
            self.log.error("Kafka exception occurred: %s", e)
            return False
        except IntegrityError as e:
            self.log.error("duplicated rows: %s", e)
            return False    
        except ValueError as e:
            self.log.error(e)
            return False
        finally:
            consumer.close()










@task(
    executor_config={'LocalExecutor': {}},
)
def receivers(topic, kafka_config, broker_id, max_messages, **kwargs):

    consumer = Consumer(kafka_config)
    consumer.subscribe([topic])
    
    data_collected = []
    message_count = 0
        
    while message_count < max_messages:
            
        msg = consumer.poll(timeout=5)  #5 segundos
                
        if msg is None:
            break
            
        if msg.error():
            self.log.error("Consumer error: %s", msg.error())
            continue
                    
        msg_obj = DECO_NSA.FromString(msg.value())
        msg_df_append = process_message(msg_obj, broker_id)
        data_collected.append(msg_df_append)
                    
        message_count += 1

    if message_count >= max_messages:
        self.log.info("MAX REACHED, Processed maximum number of messages: %s", max_messages)
    else:
        self.log.info("Processed  messages: %s", message_count)
        
    if len(data_collected) == 0:
        print('No info received')
        return True
    
    DATA_COLLECTED_DF = pd.concat(data_collected)
    print("DEBUG:")
    print(DATA_COLLECTED_DF['id'].sample(5))
    print()
    
    print('Uploading to Database: %s'%POSTGRES_IP)
    DATA_COLLECTED_DF.to_sql(POSTGRES_TABLE, POSTGRES_ENGINE, if_exists='append', index=False)     
    
    return True

@task(
    executor_config={'LocalExecutor': {}},
)
def initialization(ds=None, ti=None, **kwargs):

    print("TOPIC: %s"%KAFKA_TOPIC)
    print(ds)
 
    
    return True
    

with DAG(
    dag_id='stream_arieso',
    default_args={
        "depends_on_past": False,
        'owner': 'Vmasuero'
    },
    schedule_interval='*/15 * * * *',
    start_date=days_ago(1),
    max_active_runs= 4,
    dagrun_timeout=timedelta(minutes=10),
    tags=['development', 'arieso', 'kafka'],
    catchup=False
    ) as dag:
    
        with TaskGroup(group_id='consumers_tasks') as consumers_tasks:

            for i,broker_id in enumerate(KAFKA_NAMES[:]):
                print("adding broker: %s"%broker_id)
                
                #kafka_sensor_task = ConfluentKafkaSensor(
                #    task_id = "kafka_sensor_%s"%i,
                #    topic = KAFKA_TOPIC,
                #    kafka_config={
                #        "bootstrap.servers": "%s:9092"%broker_id, 
                #        "group.id": KAFKA_GROUPID,
                #        "auto.offset.reset": "earliest",
                #    },
                #    broker_id=broker_id,
                #    max_messages=10000,  
                #    process_message_func=process_message,
                #    mode="reschedule",  
                #    poke_interval=10,   
                #    timeout=600
                #)
    
                #kafka_sensor_task
                _kafka_config={
                    "bootstrap.servers": "%s:9092"%broker_id, 
                    "group.id": KAFKA_GROUPID,
                    "auto.offset.reset": "earliest",
                }
                receivers(KAFKA_TOPIC, _kafka_config, broker_id, 10000)
                
                
        initialization() >> consumers_tasks
        
if __name__ == "__main__":
    dag.cli()
