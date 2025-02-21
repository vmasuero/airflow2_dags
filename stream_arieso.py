from airflow import DAG
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.models import Variable

import sys
sys.path.append('/usr/lib/ViaviAnalytics')


KAFKA_TOPIC = Variable.get('KAFKA_TOPIC')#'Analytics'





@task(
    executor_config={'LocalExecutor': {}},
)
def initialization(yesterday_ds = None, ds=None, ti=None, data_interval_start=None,  **kwargs):

    
    print(KAFKA_TOPIC)
 
    
    return True
    
def get_stream_arieso(message):
    "Takes in consumed messages and prints its contents to the logs."

    key = json.loads(message.key())
    message_content = json.loads(message.value())
    pet_name = message_content["pet_name"]
    pet_mood_post_treat = message_content["pet_mood_post_treat"]
    
    print(
        f"Message #{key}: Hello {name}, your pet {pet_name} has consumed another treat and is now {pet_mood_post_treat}!"
    )
    
    return True


def get_stream_arieso_1(message):
    

    print( message.value())

    return True

with DAG(
    dag_id='stream_arieso',
    default_args={
        "depends_on_past": False,
        'owner': 'Vmasuero'
    },
    schedule_interval='@once',
    tags=['development', 'arieso', 'kafka']
    ) as dag:
    
        t_get_stream_arieso = ConsumeFromTopicOperator(
            task_id="get_stream_arieso_1",
            kafka_config_id="ARIESO_KAFKA",
            topics=[KAFKA_TOPIC],
            apply_function=get_stream_arieso_1,
        apply_function_kwargs={},
        poll_timeout=20,
        max_messages=20,
        max_batch_size=2,
        )
        
        
        initialization() #>> t_get_stream_arieso