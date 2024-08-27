from textual.app import App, ComposeResult
from textual.widgets import Input, RichLog, Header
from textual import on
from kafka import KafkaProducer, KafkaConsumer
import json
import asyncio
import threading
import time

class ChatApp(App):
    def __init__(self, user_name: str, chat_room: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.user_name = user_name
        self.chat_room = chat_room

    def compose(self) -> ComposeResult:
        # UI 구성
        yield Header()
        yield RichLog(id="log", markup=True)
        yield Input(id="input")
    
    async def on_mount(self) -> None:
        # 페이지가 마운트될 때 Kafka consumer 스레드를 시작
        self.title = f"<< {self.chat_room} >>"
        self.consumer_thread = threading.Thread(target=self.start_consumer, daemon=True)
        self.consumer_thread.start()

    def start_consumer(self):
        # Kafka consumer 초기화 및 메시지 수신
        self.consumer = KafkaConsumer(
            #bootstrap_servers=['localhost:9092'],
            bootstrap_servers=['ec2-43-203-210-250.ap-northeast-2.compute.amazonaws.com:9092'],
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
        # 본인 메시지에 markdown 적용
            log.write(f"[red]\[{data['nickname']}][/red] {data['message']} | {data['time']}")
        elif data['nickname'] == "@bot":
            log.write(f"[ansi_bright_blue]\[{data['nickname']}] {data['message']} | {data['time']}[/ansi_bright_blue]")
        else:
            log.write(f"\[{data['nickname']}] {data['message']} | {data['time']}")

    @on(Input.Submitted)
    async def on_input_submitted(self, event: Input.Submitted) -> None:
        # 메시지를 Kafka로 전송
        input_widget = self.query_one(Input)
        message = input_widget.value
        if message.strip():
            # Kafka producer 초기화 및 메시지 전송
            producer = KafkaProducer(
                #bootstrap_servers=['localhost:9092'],
                bootstrap_servers=['ec2-43-203-210-250.ap-northeast-2.compute.amazonaws.com:9092'],
                value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8')
            )
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

if __name__ == "__main__":
    chatroom = input("대화방명 : ")
    username = input("사용자명 : ")
    app = ChatApp(chat_room=chatroom, user_name=username)
    app.run()

