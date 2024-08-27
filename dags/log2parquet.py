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
    schedule_interval='@hourly',
    start_date=datetime(2024, 8, 26),
    catchup=True,
    tags=['logs','parquet'],

) as dag:
    start=EmptyOperator(task_id='start')
    end=EmptyOperator(task_id='end')     
    
    
    consumer = ConsumeFromTopicOperator(
        kafka_config_id="team4",
        #kafka_config_id="local",
		task_id="consumer",
		topics=["team4"],
		#topics=["room1","room2","room3"],
        apply_function="consumer.fun_consumer",
        #apply_function_kwargs={"prefix": "consumed:::"},
		commit_cadence="end_of_operator",
        #commit_cadence="end_of_batch",
		max_messages=15
		#max_batch_size=3,
    )

    start >> consumer >> end
        
