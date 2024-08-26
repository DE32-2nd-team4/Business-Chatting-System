from kafka import KafkaConsumer, KafkaProducer
import time
import json
import threading
import logging
import curses
import os

def send_message():
    global username
    global chatroom
    
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
        bootstrap_servers=['ec2-43-203-210-250.ap-northeast-2.compute.amazonaws.com:9092'],
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
            log_file.write(json.dumps(m_message) + '\n')  # 줄바꿈 추가
        # server/team4/chatroom/chat.log
        # m_message를 chat.log에 삽입
        producer.flush()  # 메시지 전송 완료



def receive_message():
    global chatroom
    global username
    receiver = KafkaConsumer(
            bootstrap_servers='ec2-43-203-210-250.ap-northeast-2.compute.amazonaws.com:9092',
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
        return

    finally:
        receiver.close()


def cur_win(stdscr):
    input_win = curses.newwin(1, courses.COLS, curses.LINES -1, 0)
    output_win = curses.newwin(curses.LINES -1, courses.COLS, 0, 0)


if __name__ == "__main__":
    print("채팅 프로그램 - 메시지 발신 및 수신")
    username = input("사용할 이름을 입력하세요 : ")
    chatroom = input("대화방 이름을 입력하세요 : ")

    curses.wrapper(cur_win)
    
    consumer_thread = threading.Thread(target=receive_message)
    producer_thread = threading.Thread(target=send_message)

    consumer_thread.start()
    producer_thread.start()

