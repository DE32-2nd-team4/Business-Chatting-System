#from confluent_kafka import Message

def fun_consumer(messages):
    print(f"Type of message: {type(messages)}")
    print(f"Dir of message: {dir(messages)}")
    print(f"{messages.topic()} ----------- {messages.value()}")

    data = []
    for message in messages:  # messages는 여러 메시지를 담고 있는 객체이므로 반복문으로 처리
        decoded_message = json.loads(message.value.decode('utf-8'))
        data.append({
            'topic': message.topic,
            'nickname': decoded_message['nickname'],
            'message': decoded_message['message'],
            'time': decoded_message['time']
        })
    print(data)

    
