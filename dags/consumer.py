import pandas as pd
import json
import os

def fun_consumer(messages):
    decoded_data = messages.value().decode('utf-8')
    print(f"------------------{decoded_data}")
    json_data = json.loads(decoded_data)

    home_dir = os.path.expanduser("~")
    file_name = f"{json_data['time'][:10]}.txt"
    print('^^^^^^^^^^^^^^')
    print(file_name)
    #file_path = os.path.join(os.path.expanduser("~"), 'code','Business-Chatting-System', 'data','json', file_name, 'data.json')
    #print('*'*300)
    #print(file_path)

    # Step 1: 파일이 있는지 확인하고, 없으면 생성하여 내용 추가
    #with open(file_path, 'a') as file:
    #    file.write(json_data)

    # "time" 필드만 추출
    #time_value = json_data['time']
    #print(f"Time: {time_value}")

   