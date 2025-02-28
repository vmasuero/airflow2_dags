from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

from airflow.utils.dates import days_ago


from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient

from sqlalchemy import create_engine
from sqlalchemy.engine import reflection


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
POSTGRES_ENGINE = create_engine(POSTGRES_EP)


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

def process_message(msg_obj, broker_id:str) -> pd.DataFrame:
    
    def message_to_dict(msg):

        result = {}
        for field in msg.DESCRIPTOR.fields:
            field_name = field.name

            if field.label != field.LABEL_REPEATED:
                if field.cpp_type == field.CPPTYPE_MESSAGE:
                
                    if not msg.HasField(field_name):
                        continue
                        
                    value = getattr(msg, field_name)
                    result[field_name] = message_to_dict(value)
                    
                else:
                    result[field_name] = getattr(msg, field_name)
                    
            else:
                value_list = getattr(msg, field_name)
                
                if value_list:
                    result[field_name] = []
                    
                    for item in value_list:
                        if field.cpp_type == field.CPPTYPE_MESSAGE:
                            result[field_name].append(message_to_dict(item))
                        else:
                            result[field_name].append(item)
        return result

    def dict_to_dataframe(msg_dict:dict):
        common_fields = {k: v for k, v in msg_dict.items() if k not in ['NrCells', 'NrErab']}
    
        rows = []
        has_cells = 'NrCells' in msg_dict and msg_dict['NrCells']
        has_erab = 'NrErab' in msg_dict and msg_dict['NrErab']
        
        if has_cells and has_erab:
            for cell in msg_dict['NrCells']:
                
                for erab in msg_dict['NrErab']:
                    row = common_fields.copy()
                    for k, v in cell.items():
                        row[f"NrCells_{k}"] = v
                        
                    for k, v in erab.items():
                        row[f"NrErab_{k}"] = v
                        
                    rows.append(row)
                    
            return pd.DataFrame(rows)
            
        elif has_cells:
            for cell in msg_dict['NrCells']:
                row = common_fields.copy()
                
                for k, v in cell.items():
                    row[f"NrCells_{k}"] = v
                    
                rows.append(row)
                
            return pd.DataFrame(rows)
            
        else:
            return pd.DataFrame()
  
    _msg_dict = message_to_dict(msg_obj)
    _msg_df =  dict_to_dataframe(_msg_dict)

    if _msg_df.empty:
        print('Error con mensaje:')
        print(_msg_dict)
        return pd.DataFrame()
        
    _msg_df.columns = _msg_df.columns.str.lower() 
    _msg_df = _msg_df.round(0)           
    _msg_df['date_starttime'] = pd.to_datetime(_msg_df['segmentstarttime'], unit='ms', utc=True)
    _msg_df['id'] = _msg_df.apply(lambda x: str(x.nrcells_nrcelllabel) + '-' + str(x.imsi) + '-' + str(x.segmentstarttime), axis=1)
    _msg_df = _msg_df[REQUIRED_COLS + ['id','date_starttime']]
    _msg_df['broker_id'] = broker_id
    
    del _msg_df['segmentstarttime']
    
    return _msg_df



class ConfluentKafkaSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self, topic, kafka_config, max_messages=1, process_message_func=None, broker_id, *args, **kwargs):

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
            
                msg = consumer.poll(timeout=1.0)
                
                if msg is None:
                    break
                    
                if msg.error():
                    self.log.error("Consumer error: %s", msg.error())
                    break
                    
                _msg_obj = DECO_NSA.FromString(msg.value())
                _msg_df_append = self.process_message_func(_msg_obj,self.broker_id)
                data_collected.append(_msg_df_append)
                    
                self.message_count += 1

            if self.message_count >= self.max_messages:
                self.log.info("Processed maximum number of messages: %s", self.max_messages)
            
            DATA_COLLECTED_DF = pd.concat(data_collected)
            
            print('Uploading to Database: %s'%POSTGRES_IP)
            try:
                DATA_COLLECTED_DF.to_sql(POSTGRES_TABLE, POSTGRES_ENGINE, if_exists='append', index=False)     
            except IntegrityError:
                print('duplicated rows')
            except ValueError as e:
                print(e)
                
        except KafkaException as e:
            self.log.error("Kafka exception occurred: %s", e)
            return False
        finally:
            consumer.close()

@task(
    executor_config={'LocalExecutor': {}},
)
def initialization(ds=None, ti=None, **kwargs):

    
    print("TOPIC %s"%KAFKA_TOPIC)
    print(ds)
 
    
    return True
    

with DAG(
    dag_id='stream_arieso',
    default_args={
        "depends_on_past": False,
        'owner': 'Vmasuero'
    },
    schedule_interval='@once',
    start_date=days_ago(1),
    tags=['development', 'arieso', 'kafka'],
    catchup=False
    ) as dag:
    
        #with TaskGroup(group_id='dw_tasks') as dw_tasks:

        #for i,server_conn in enumerate(PM_HUAWEI_SERVERS[:2]):
    
        #kafka_sensor_task = ConfluentKafkaSensor(
        #    task_id="confluent_kafka_sensor",
        #    topic="my_topic",  # Replace with your Kafka topic
        #    kafka_config={
        #        "bootstrap.servers": "localhost:9092",  # Your Kafka broker
        #        "group.id": "airflow_sensor_group",
        #        "auto.offset.reset": "earliest",
        #    },
        #    max_messages=5,  # Set the maximum number of messages to process
        #    process_message_func=process_message,
        #    mode="reschedule",  # Recommended to free up worker slots while waiting
        #    poke_interval=10,   # Poll every 10 seconds
        #    timeout=300         # Sensor will time out after 300 seconds if condition isn't met
        #    )
    
        initialization() 
        
if __name__ == "__main__":
    dag.cli()