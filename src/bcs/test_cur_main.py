import curses
import threading
import time
from kafka import KafkaProducer, KafkaConsumer
import json
import os

def send_message(input_win, height, width, username, chatroom):
    # Kafka 프로듀서 초기화
    producer = KafkaProducer(
            bootstrap_servers=['ec2-43-203-210-250.ap-northeast-2.compute.amazonaws.com:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    # 입력 윈도우 생성

    try:
        while True:
            input_win.clear()
            input_win.addstr(0, 0, "입력: ")
            input_win.refresh()

            user_input = input_win.getstr().decode('utf-8')

            if user_input.lower() == 'exit':
                producer.close()
                break
    
            # Kafka로 메시지 전송
            message = {'nickname': username, 'message': user_input, 'time': time.strftime('%Y-%m-%d %H:%M:%S')}
            producer.send(chatroom, value=message)
            producer.flush()  # 메시지 전송 완료

    except KeyboardInterrupt:
        producer.close()
        return

def receive_message(output_win, height, width, username, chatroom):
    # Kafka 컨슈머 초기화
    consumer = KafkaConsumer(
        bootstrap_servers=['ec2-43-203-210-250.ap-northeast-2.compute.amazonaws.com:9092'],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    consumer.subscribe([chatroom])

    try:
        for message in consumer:
            data = message.value
            output_win.addstr(f"{data['nickname']} >> {data['message']} | {data['time']}\n") 
            output_win.refresh()
    except KeyboardInterrupt:
        print("채팅 종료")
        consumer.close()
        return
        
stdscr = curses.initscr()
stdscr.clear()

# 사용자 이름과 채팅방 입력 받기
stdscr.addstr(0, 0, "사용할 이름을 입력하세요: ")
username = stdscr.getstr().decode('utf-8')

stdscr.addstr(1, 0, "대화방 이름을 입력하세요: ")
chatroom = stdscr.getstr().decode('utf-8')

stdscr.refresh()

height, width = stdscr.getmaxyx()
input_win = curses.newwin(1, width, height - 1, 0)
output_win = curses.newwin(height - 1, width, 0, 0)

input_win.scrollok(True)
output_win.scrollok(True)

# 프로듀서와 컨슈머 스레드 시작
producer_thread = threading.Thread(target=send_message, args=(input_win, height, width, username, chatroom))
consumer_thread = threading.Thread(target=receive_message, args=(output_win, height, width, username, chatroom))
    
producer_thread.start()
consumer_thread.start()
    
# 스레드가 종료될 때까지 기다리기
producer_thread.join()
consumer_thread.join()

curses.endwin()
