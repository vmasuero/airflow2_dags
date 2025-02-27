from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.models import Variable

from airflow.utils.dates import days_ago


from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient



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
    
    print('MESSAGE')
    print( message.value())

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