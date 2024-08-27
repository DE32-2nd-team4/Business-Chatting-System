#from confluent_kafka import Message

def fun_consumer(messages):
    print('*'*300)
    print(f"Type of message: {type(messages)}")
    print('='*300)
    print(f"Dir of message: {dir(messages)}")
    print('/'*300)
    print(f"{messages.topic()} ----------- {messages.value()}")

    