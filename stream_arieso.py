from airflow import DAG
from airflow.providers.apache.kafka.hooks.kafka import KafkaHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task

@task(
    executor_config={'LocalExecutor': {}},
)
def arieso_stream_lte():
    kafka_hook = KafkaHook(kafka_config_id="ARIESO_KAFKA")
    messages = kafka_hook.consume(topics=["Analytics"], num_messages=2, timeout=10)
    
    for msg in messages:
        print(f"Mensaje recibido: {msg.value}")



with DAG(
    dag_id='stream_arieso',
    #schedule_interval= "@daily",
    default_args={
        "depends_on_past": False,
        'owner': 'Vmasuero',
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    start_date= days_ago(1),
    catchup=True,
    tags=['development', 'arieso', 'kafka'],
    max_active_runs=1
    ) as dag:
    
        arieso_stream_lte()