import os
from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator


from airflow.operators.python import (
        PythonOperator,
        PythonVirtualenvOperator,
        BranchPythonOperator
)

with DAG(
    'log2parquet',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=3,
    description='movie_data_spark',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 8, 25),
    catchup=True,
    tags=['logs','parquet'],
) as dag:
    start=EmptyOperator(task_id='start')
    end=EmptyOperator(task_id='end')     
    
    
    consumer = ConsumeFromTopicOperator(
        kafka_config_id="team4",
		task_id="consumer",
		topics=["Room1","Room2","Room3","team4-room1","team4-room2","team4"],
        apply_function="consumer.fun_consumer",
		commit_cadence="end_of_operator",
		max_messages=100,
		max_batch_size=16
    )


    start >> consumer >> end
        
