from kafka import KafkaConsumer, KafkaProducer
import time
import json
import threading
import logging
import os


def import_ip():
    try:
        with open('config/ip', 'r') as f:
            config_lines = f.readlines()

        bootstrap_servers = ""
        for line in config_lines:
            if line.startswith("server address:"):
                bootstrap_servers = line.split(":")[1].strip().replace("'", "")
            elif line.startswith("port:"):
                port = line.split(":")[1].strip().replace("'", "")
                bootstrap_servers += ":" + port
        return bootstrap_servers
    except FileNotFoundError:
        print("Error: config/ip 파일을 찾을 수 없습니다.")
        exit(1)
    except Exception as e:
        print(f"Error: config/ip 파일을 읽는 중 오류가 발생했습니다: {e}")
        exit(1)

def moduleListener():
    #KafkaConsumer로 특정 topic을 듣고
    #Airflow에서 Producing하는 메세지는 topic를 'team4_system'으로 지정
    #즉, 본 Listener에서 듣는 topic도 'team4_system'으로 지정
    #이렇게 들은 메세지를 리턴함.

    address = import_ip()
    print(f"address import complete : {address}")
    subscribe_topic = ['team4_aleam']
    print(f"topic : {subscribe_topic}")
    listener = KafkaConsumer(
            bootstrap_servers=address,
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            )
    print("listener ready")
    listener.subscribe(subscribe_topic)

    for message in listener:
        print(f"message listen : {message.value['message']} | {message.value['time']}") 
        data = {'message': message.value['message'], 'time': message.value['time']}
        moduleSpeaker(data)


    

def moduleSpeaker(message):
    #TODO
    #KafkaProducer로 메세지를 전송
    #메세지를 makeMessage를 통해 리턴받고 그것을 그대로 Producer를 통해 발송
    address = import_ip()
    topic = 'team4'
    producer = KafkaProducer (
        bootstrap_servers=address,
        value_serializer=lambda x:json.dumps(x,ensure_ascii=False).encode('utf-8'),
        )
    message_i = message['message'][4:]
    time = message['time']
    m_message = {'nickname': '@aleambot', 'message': message_i, 'time':time}
    print(f"message ready to send {topic} : {m_message}")
    producer.send(topic, value=m_message)
    producer.flush()
    

moduleListener()
