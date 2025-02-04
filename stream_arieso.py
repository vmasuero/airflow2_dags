from airflow import DAG
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task

KAFKA_TOPIC = 'Analytics'

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
    
    print(message)

    return True

with DAG(
    dag_id='stream_arieso',
    default_args={
        "depends_on_past": False,
        'owner': 'Vmasuero'
    },
    start_date= days_ago(1),
    catchup=True,
    tags=['development', 'arieso', 'kafka'],
    max_active_runs=1
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
        
        
        t_get_stream_arieso