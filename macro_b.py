from kafka import KafkaProducer
import time
import json
import threading
import random

def msg_throw():
    user = 'USER-C'
    chatroom = 'team4-room2'

    producer = KafkaProducer (
            bootstrap_servers=['ec2-43-203-210-250.ap-northeast-2.compute.amazonaws.com:9092'],
            value_serializer=lambda x:json.dumps(x).encode('utf-8'),                           batch_size=16384,
            )

    while(True):
        message = macromessage()
        m_message = {'nickname':user,'message':message,'time':time.ctime()}
        producer.send(chatroom, value=m_message)
        time.sleep(2)
        producer.flush()

def macromessage():
    messages = [
    "요즘 고민이 있는데, 혹시 들어주실 수 있을까요?",
    "꿈을 이루기 위해 어떤 노력을 하고 계세요?",
    "인생에서 가장 중요하다고 생각하는 가치는 무엇인가요?",
    "스트레스 받을 때 어떻게 극복하세요?",
    "가장 좋아하는 여행지는 어디인가요? 그 이유는 무엇인가요?",
    "만약 시간을 되돌릴 수 있다면, 언제로 돌아가고 싶으세요?",
    "최근에 감명 깊게 읽은 책이나 본 영화가 있나요?",
    "나에게 가장 큰 영향을 준 사람은 누구인가요?",
    "앞으로 꼭 이루고 싶은 목표가 있나요?",
    "행복이란 무엇이라고 생각하세요?"
]
    return random.choice(messages)

if __name__ == "__main__":
    throw = threading.Thread(target=msg_throw)
    throw.start()
