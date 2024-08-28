from kafka import KafkaConsumer, KafkaProducer
import time
import json
import threading
import logging
import os

# 이 밑에 부분 그냥 복사해서 붙여넣어주세요!
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
    

def send_message():
    global username
    global chatroom
    
    # bootstrap_server 받는 함수
    address = import_ip()

    log_file_path = f"~/atmp/chatdata/{chatroom}/chat.log"
    log_file_path = os.path.expanduser(log_file_path)
    # 로그 파일 경로 (EC2 서버의 team4 폴더 아래 chatroom 폴더)
    chatroom_dir = os.path.dirname(log_file_path)
    if not os.path.exists(chatroom_dir):
        os.makedirs(chatroom_dir)

# chat.log 파일 터치 (존재하지 않을 경우 생성)
    if not os.path.exists(log_file_path):
        open(log_file_path, 'a').close()  # 파일 생성
    producer = KafkaProducer (
        bootstrap_servers=address,
        value_serializer=lambda x:json.dumps(x).encode('utf-8'),
        )


    while(True):
        message = input(" ")  # 사용자 입력 받기
        if message == "exit":
            producer.close()
            break
        m_message = {'nickname': username, 'message': message, 'time':time.strftime('%Y-%m-%d %H:%M:%S')}
        producer.send(chatroom, value=m_message)
        with open(log_file_path, 'a') as log_file:
            log_file.write(json.dumps(m_message) + '\n')
            # 줄바꿈 추가
        # server/team4/chatroom/chat.log
        # m_message를 chat.log에 삽입
        producer.flush()  # 메시지 전송 완료



def receive_message():
    global chatroom
    global username

    # bootstrap_server 받는 함수
    address = import_ip()

    receiver = KafkaConsumer(
            bootstrap_servers=address,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            )
    receiver.subscribe([chatroom])
    
    try:
        for message in receiver:
            data = message.value
            if username == data['nickname']:
                print(f"{data['time']}|{data['message']}")
            else:
                print(f"{data['nickname']} >> {data['message']} | {data['time']}")

    except KeyboardInterrput:
        print("채팅 종료")

    finally:
        receiver.close()


def run():
    # if '__name__' == '__main__'의 경우는
    # pdm의 project.script에서 실핼하기 어렵습니다. 
    global username
    global chatroom

    username = input("사용자명을 입력하세요. : ")
    chatroom = input("접속할 대화방명을 입력하세요. : ")

    consumer_thread = threading.Thread(target=receive_message)
    producer_thread = threading.Thread(target=send_message)

    consumer_thread.start()
    producer_thread.start()

