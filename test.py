
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
    }

dag = DAG('dummy_operator_example', default_args=default_args, schedule_interval='@daily')

start_task = DummyOperator(task_id='start_task', dag=dag)

task_1 = DummyOperator(task_id='task_1', dag=dag)
task_2 = DummyOperator(task_id='task_2', dag=dag)

end_task = DummyOperator(task_id='end_task', dag=dag)

start_task >> task_1 >> task_2 >> end_task
