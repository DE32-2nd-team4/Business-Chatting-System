from kafka import KafkaConsumer, KafkaProducer
import time
import json
import threading
import logging
import os
import requests
import chatbot1 as cb1
import chatbot2 as cb2
import chatbot3 as cb3
import chatbot4 as cb4
# pip install schedule
import schedule

def is_integer(s):
    try:
        int(s)
        return True
    except ValueError:
        return False

def receive_message():
    global chatroom
    global username
    global global_command

    receiver = KafkaConsumer(
            bootstrap_servers='ec2-43-203-210-250.ap-northeast-2.compute.amazonaws.com:9092',
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            )
    receiver.subscribe([chatroom])

    try:
        for message in receiver:
            data = message.value
            # chatbot 실행
            if data['message'][:4] == "@bot":
                message = data['message'][5:]
                #chatbot start
                to_bot_data = [data['topic'], data['nickname'], message]
                bot_scheduler(to_bot_data)

            elif data['message'][:4] == "@bot" and is_integer(data['message'][5:]):
                m, search_word, command, movie_cd_list = global_command
                real_movie_cd = movie_cd_list[int(data['message'][5:]) - 1]
                to_bot_data2 = [data['topic'], data['nickname'], real_movie_cd, command]
                bot2_scheduler(to_bot_data2)

    except KeyboardInterrput:
        print("채팅 종료")

    finally:
        receiver.close()

def bot_scheduler():
    schedule.every(10).seconds.do(call_bot, 0, to_bot_data)
    schedule.every(10).seconds.do(call_bot, 1, to_bot_data)
    schedule.every(10).seconds.do(call_bot, 2, to_bot_data)
    schedule.every(10).seconds.do(call_bot, 3, to_bot_data)

# chatbotFindMovie
def bot2_scheduler(real_movie_cd, command):
    schedule.every(10).seconds.do(call_bot, 0, to_bot_data2)
    schedule.every(10).seconds.do(call_bot, 1, to_bot_data2)
    schedule.every(10).seconds.do(call_bot, 2, to_bot_data2)
    schedule.every(10).seconds.do(call_bot, 3, to_bot_data2)

def call_bot(bot_id, to_bot_data):
    if bot_id == 0:
        cb1.chatbot(to_bot_data)
    elif bot_id == 1:
        cb2.chatbot(to_bot_data)
    elif bot_id == 2:
        cb3.chatbot(to_bot_data)
    elif bot_id == 3:
        cb4.chatbot(to_bot_data)
    else:
        print("잘못된 bot_id입니다.")

def call_bot2(bot_id, to_bot_data2):
    if bot_id == 0:
        cb1.chatbotFindMovie(to_bot_data2)
    elif bot_id == 1:
        cb2.chatbotFindMovie(to_bot_data2)
    elif bot_id == 2:
        cb3.chatbotFindMovie(to_bot_data2)
    elif bot_id == 3:
        cb4.chatbotFindMovie(to_bot_data2)
    else:
        print("잘못된 bot_id입니다.")
