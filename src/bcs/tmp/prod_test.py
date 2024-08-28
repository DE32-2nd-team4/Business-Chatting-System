from textual.app import App, ComposeResult
from textual.widgets import Input, RichLog, Header
from textual import on
from kafka import KafkaProducer, KafkaConsumer
import json
import asyncio
import threading
import time
import re

class ChatApp(App):
    def __init__(self, user_name: str, chat_room: str, server: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.user_name = user_name
        self.chat_room = chat_room
        self.server = server

    def compose(self) -> ComposeResult:
        # UI 구성
        yield Header(show_clock=True)
        yield RichLog(id="log", markup=True)
        yield Input(id="input")
    
    async def on_mount(self) -> None:
        # 페이지가 마운트될 때 Kafka consumer 스레드를 시작
        self.title = f"🏠 {self.chat_room}"
        self.consumer_thread = threading.Thread(target=self.start_consumer, daemon=True)
        self.consumer_thread.start()

    def start_producer(self):
        producer = KafkaProducer(
                bootstrap_servers=[self.server],
                value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8')
        )
        return producer

    def start_consumer(self):
        # Kafka consumer 초기화 및 메시지 수신
        self.consumer = KafkaConsumer(
            bootstrap_servers=[self.server],
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        self.consumer.subscribe([self.chat_room])

        try:
            for message in self.consumer:
                data = message.value
                asyncio.run(self.update_msg(data))
        except KeyboardInterrupt:
            Print("채팅을 종료합니다...")
        finally:
            self.consumer.close()

    async def update_msg(self, data: dict) -> None:
        # 메시지 추가
        log = self.query_one(RichLog)

        if data['nickname'] == self.user_name:
            log.write(f"[{data['time']}] [red]{data['nickname']}[/] {data['message']}")
        elif data['nickname'].startswith('@'):
            log.write(f"[{data['time']}] [blue]🤖 {data['nickname']}[/] {data['message']}")
        else:
            log.write(f"[{data['time']}] [#C0E8D5]{data['nickname']}[/] {data['message']}")

    @on(Input.Submitted)
    async def on_input_submitted(self, event: Input.Submitted) -> None:
        # 메시지를 Kafka로 전송
        input_widget = self.query_one(Input)
        message = input_widget.value
        if message.strip():
            # Kafka producer 초기화 및 메시지 전송
            producer = start_producer() #KafkaProducer(
                #bootstrap_servers=[self.server],
                #value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8')
            #)
            msg = {'nickname': self.user_name, 'message': message, 'time': time.strftime('%Y-%m-%d %H:%M:%S')}
            producer.send(self.chat_room, value=msg)
            producer.flush()  # 메시지 전송 완료
            producer.close()  # 프로듀서 종료
            
            # 입력 필드 초기화
            input_widget.value = ""

    async def on_unmount(self) -> None:
        # 종료 시 Kafka consumer 종료 및 스레드 종료
        if hasattr(self, 'consumer'):
            self.consumer.close()
        if hasattr(self, 'consumer_thread'):
            self.consumer_thread.join()

    def run(self) -> None:
        try:
            super().run()
        except KeyboardInterrupt:
            print("채팅을 종료합니다...")
            self.exit()

def is_valid_username(username):
    # 정규 표현식으로 특수문자를 확인 (알파벳과 숫자만 허용)
    return re.match(r'^[a-zA-Z0-9]+$', username) is not None

if __name__ == "__main__":
    server = "ec2-43-203-210-250.ap-northeast-2.compute.amazonaws.com:9092"
    chatroom = "team4"
    username = "j25ng"

#    server = input("서버주소 : ")
#    chatroom = input("대화방명 : ")
#    while True:
#        username = input("사용자명 : ")
#        if is_valid_username(username):
#            break
#        else:
#            print("사용자명에 특수문자가 포함되어 있습니다. 다시 입력해주세요.")

    app = ChatApp(chat_room=chatroom, user_name=username, server=server)
    app.run()

