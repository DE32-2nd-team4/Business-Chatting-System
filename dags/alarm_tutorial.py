from airflow import DAG
import time
import json
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
# DAG 정의
with DAG(
    dag_id='systemAleamTestDag',  # DAG의 고유 ID
    schedule_interval='@once',  # 실행 주기 (매일 자정)
    start_date=datetime(2024, 8, 28),
    catchup=False,  # 과거 날짜 실행 여부 (False: 과거 실행 안 함)

    ) as dag:

    def fail_aleam(**context):
        message = "@bot Test_task returned fail @Jun"
        topic = "team4_system"
        address = "ec2-43-203-210-250.ap-northeast-2.compute.amazonaws.com:9092"
        producer = KafkaProducer(
        bootstrap_servers=address,
        value_serializer=lambda x:json.dumps(x,ensure_ascii=False).encode('utf-8'),
        )
        time_str = context['execution_date'].strftime('%Y-%m-%d %H:%M:%S')
        m_message = {'nickname': '@systembot', 'message': message, 'time': time_str }
        producer.send(topic, value=m_message)
        producer.flush()


    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    task_a = PythonOperator(
            task_id='task.a',
            python_callable=fail_aleam,
            )

    start >> task_a >> end
        
