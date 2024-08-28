import os
import json
import time
import pandas as pd
from kafka import KafkaConsumer,TopicPartition
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
        'depends_on_past': True,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=3,
    description='chat audit',
    schedule_interval='@hourly',
    start_date=datetime(2024, 8, 28),
    catchup=True,
    tags=['logs','parquet'],

) as dag:
    start=EmptyOperator(task_id='start')
    end=EmptyOperator(task_id='end')     

    

    def fun_con(**kwargs):
        c = KafkaConsumer(
            'team4',
            bootstrap_servers=['ec2-43-203-210-250.ap-northeast-2.compute.amazonaws.com:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='E',
            consumer_timeout_ms=30000,
            fetch_max_wait_ms=30000,
            value_deserializer = lambda x: json.loads(x.decode('utf-8'))
        )

        partition = TopicPartition('team4', 0)  # 특정 파티션 지정
        #c.assign([partition])
        
        for partition in c.assignment():
            last_committed_offset = c.committed(partition)
            if last_committed_offset is not None:
                # 마지막 커밋된 오프셋으로 이동
                c.seek(partition, last_committed_offset)
            else:
                # 커밋된 오프셋이 없을 경우, 가장 오래된 오프셋으로 이동
                c.seek_to_beginning(partition)

        df = pd.DataFrame(columns=['nickname', 'message', 'time'])
        #cnt = 0
        for m in c:
            #cnt +=1
            #if cnt==5:
            #    break
            try:
                nickname = m.value.get('nickname', 'N/A')
                message = m.value.get('message', 'N/A')
                time_value = m.value.get('time', 'N/A')
                #print(f"------------------{m.value['nickname','message','time']}")          
                #print(f"------------------{m.value['nickname']}   {m.value['message']}    {m.value['time']}")           
                print(f"{nickname}   {message}   {time_value}")
                data = [[nickname], [message], [time_value]]
                
                df_data = pd.DataFrame([[nickname, message, time_value]], columns=['nickname', 'message', 'time'])
                df = pd.concat([df, df_data], ignore_index=True)
               
            except KeyError as e:
                print(f"KeyError: {e}")
            except UnicodeEncodeError as e:
                print(f"Unicode encoding error: {e}")
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
        
            print("*"*1000)
            

        home_dir = os.path.expanduser("~")
        file_path = os.path.join(os.path.expanduser("~"), 'code','Business-Chatting-System', 'data', 'csv', 'data.csv')

        # 파일이 있는지 확인하고, 없으면 생성하여 내용 추가
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        df.to_csv(file_path, mode='a', header=not os.path.exists(file_path), index=False)

    def fun_parquet():
        print('parquet')
        home_dir = os.path.expanduser("~")

        #csv 파일 읽기
        csv_file_path = os.path.join(os.path.expanduser("~"), 'code','Business-Chatting-System', 'data', 'csv', 'data.csv')
        df = pd.read_csv(csv_file_path) 

        parquet_file_path = os.path.join(os.path.expanduser("~"), 'code','Business-Chatting-System', 'data', 'parquet', 'p.parquet')
        
        # 디렉토리가 존재하지 않으면 생성
        #os.makedirs(os.path.dirname(parquet_file_path), exist_ok=True)


        if os.path.exists(parquet_file_path):
            # 기존 Parquet 파일 읽기
            df_existing = pd.read_parquet(parquet_file_path, engine='pyarrow')
            # 기존 데이터프레임과 새 데이터프레임을 결합
            df_combined = pd.concat([df_existing, df], ignore_index=True)
        else:
            # 디렉토리가 존재하지 않으면 생성
            os.makedirs(os.path.dirname(parquet_file_path), exist_ok=True)

            # 새 파일을 위한 데이터프레임 사용
            df_combined = df

        # Parquet 파일로 저장
        df_combined.to_parquet(parquet_file_path, index=False, engine='pyarrow')


       
        
        
    consumer = PythonOperator(
        task_id="c",
        python_callable=fun_con,
    )

    to_parquet = PythonOperator(
        task_id="to.parquet",
        python_callable=fun_parquet

    )


    start >> consumer >> to_parquet >> end
        
