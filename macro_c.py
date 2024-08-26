from kafka import KafkaProducer
import time
import json
import threading
import random

def msg_throw():
    user = 'USER-D'
    chatroom = 'team4-room1'

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
    "잠깐만요, 제가 다시 확인해보고 말씀드리겠습니다.",
    "그 점에 대해서는 좀 더 논의가 필요할 것 같습니다.",
    "흥미로운 아이디어입니다. 좀 더 구체적으로 설명해주시겠어요?",
    "모두 의견을 자유롭게 말씀해주세요.",
    "이 안건에 대해서 결정을 내려야 할 시간입니다.",
    "다음 회의까지 이 문제에 대한 해결 방안을 찾아오겠습니다.",
    "제 생각에는 이렇게 접근하는 것이 좋을 것 같습니다.",
    "이 부분에 대해서는 다른 팀과 협력이 필요할 것 같습니다.",
    "회의 시간이 예상보다 길어지고 있습니다. 핵심 내용을 중심으로 빠르게 진행하겠습니다.",
    "회의 내용을 요약하여 이메일로 보내드리겠습니다."
    ]
    return random.choice(messages)

if __name__ == "__main__":
    throw = threading.Thread(target=msg_throw)
    throw.start()
