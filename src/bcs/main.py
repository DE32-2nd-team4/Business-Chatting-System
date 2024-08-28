from kafka import KafkaProducer, KafkaConsumer
from textual.app import App, ComposeResult
from textual.widgets import Input, RichLog, Header
from textual import on
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

        self.producer = KafkaProducer(
            bootstrap_servers=[self.server],
            value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8')
        )

        msg = {
                'nickname': "sys",
                'message': f"--- {self.user_name}님이 입장하였습니다. ---",
                'time': time.strftime('%Y-%m-%d %H:%M:%S')
        }

        self.producer.send(self.chat_room, value=msg)
        self.producer.flush()  # 메시지 전송 완료
        
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

        if data['nickname'] == "sys":
            log.write(f"[#999999]{data['message']}[/]")
        elif data['nickname'] == self.user_name:
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
            msg = {
                    'nickname': self.user_name,
                    'message': message,
                    'time': time.strftime('%Y-%m-%d %H:%M:%S')
            }
            self.producer.send(self.chat_room, value=msg)
            self.producer.flush()  # 메시지 전송 완료
            
            # 입력 필드 초기화
            input_widget.value = ""

    async def on_unmount(self) -> None:
        if hasattr(self, 'consumer'):
            self.consumer.close()

        msg = {
            'nickname': "sys",
            'message': f"--- {self.user_name}님이 퇴장하였습니다. ---",
            'time': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        self.producer.send(self.chat_room, value=msg)
        self.producer.flush()  # 메시지 전송 완료

        if hasattr(self, 'producer'):
            self.producer.close()

        if hasattr(self, 'consumer_thread'):
            self.consumer_thread.join()

    def run(self) -> None:
        try:
            super().run()
        except KeyboardInterrupt:
            print("채팅을 종료합니다...")
            self.exit()

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

def is_valid_username(username):
    # 정규 표현식으로 특수문자를 확인 (알파벳과 숫자만 허용)
    return re.match(r'^[a-zA-Z0-9가-힣]+$', username) is not None

if __name__ == "__main__":
    server = import_ip() 
    chatroom = input("대화방명 : ")
    while True:
        username = input("사용자명 : ")
        if is_valid_username(username):
            break
        else:
            print("사용자명에 특수문자가 포함되어 있습니다. 다시 입력해주세요.")

    app = ChatApp(chat_room=chatroom, user_name=username, server=server)
    app.run()

