from kafka import KafkaProducer
import time
import json
import threading
import random

def msg_throw():
    user = 'USER-B'
    chatroom = 'team4-room2'

    producer = KafkaProducer (
            bootstrap_servers=['ec2-43-203-210-250.ap-northeast-2.compute.amazonaws.com:9092'],
            value_serializer=lambda x:json.dumps(x).encode('utf-8'),                           batch_size=16384,
            )

    while(True):
        message = macromessage()
        m_message = {'nickname':user,'message':message,'time':time.ctime()}
        producer.send(chatroom, value=m_message)
        time.sleep(3)
        producer.flush()

def macromessage():
    messages = [
    "오늘 커피 왜 이렇게 맛있지?",
    "점심 뭐 먹을지 고민되네요...",
    "주말에 뭐 할지 벌써부터 설레요!",
    "요즘 날씨가 너무 변덕스러워요. 감기 조심하세요!",
    "퇴근하고 뭐 하세요?",
    "집에 가면 냥냥이가 기다리고 있어서 힘이 나요!",
    "최근에 재미있는 유튜브 채널 발견했는데, 같이 볼래요?",
    "오늘따라 시간이 너무 안 가는 것 같아요...",
    "이번 주말에 캠핑 가려고 하는데, 같이 갈래요?",
    "혹시 추천해주실 만한 맛집 있나요?"
]
    return random.choice(messages)

if __name__ == "__main__":
    throw = threading.Thread(target=msg_throw)
    throw.start()
