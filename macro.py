from kafka import KafkaProducer
import time
import json
import threading
import random

def msg_throw():
    user = 'USER-A'
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
        "잠시만요, 생각 좀 정리하고 말씀드릴게요.",
        "그 부분은 다시 한번 검토해 보겠습니다.",
        "좋은 의견 감사합니다. 메모해 두겠습니다.",
        "다른 분들의 의견은 어떠신가요?",
        "이 문제에 대해서는 추가적인 논의가 필요할 것 같습니다.",
        "결론을 내리기 전에 좀 더 자세히 살펴봐야겠습니다.",
        "제안해 주신 내용을 반영하여 수정해 보겠습니다.",
        "모두 동의하시면 다음 안건으로 넘어가겠습니다.",
        "시간이 좀 지났네요. 잠시 휴식 시간을 갖겠습니다.",
        "회의록은 제가 정리해서 공유하도록 하겠습니다."
    ]
    return random.choice(messages)

if __name__ == "__main__":
    throw = threading.Thread(target=msg_throw)
    throw.start()
