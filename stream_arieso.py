from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.models import Variable

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

class ConfluentKafkaSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self, topic, kafka_config, max_messages=1, process_message_func=None, *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.topic = topic
        self.kafka_config = kafka_config
        self.max_messages = max_messages
        self.process_message_func = process_message_func
        self.message_count = 0  

    def poke(self, context):
        self.log.info("Polling Kafka topic: %s (processed %s/%s messages)", self.topic, self.message_count, self.max_messages)
        

        consumer = Consumer(self.kafka_config)
        consumer.subscribe([self.topic])
        
        try:

            while self.message_count < self.max_messages:
            
                msg = consumer.poll(timeout=1.0)
                
                if msg is None:
                    break
                    
                if msg.error():
                    self.log.error("Consumer error: %s", msg.error())
                    break
                    

                message = msg.value().decode("utf-8")
                self.log.info("Received message: %s", message)
                if self.process_message_func:
                    self.process_message_func(message)
                self.message_count += 1

            if self.message_count >= self.max_messages:
                self.log.info("Processed maximum number of messages: %s", self.max_messages)
                return True
            return False  # Continue poking if we haven't reached the target count.
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
    
        initialization() 
        
if __name__ == "__main__":
    dag.cli()