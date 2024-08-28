from kafka import KafkaConsumer, KafkaProducer
import time
import json
import threading
import logging
import os
import requests
import sys

global_command = {}
bot_nic = ""
def getkey():
    key = "82ca741a2844c5c180a208137bb92bd7"
    return key

def getMovieNameBaseUrl():
    key = getkey()
    movie_name_base_url=f"http://kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieList.json?key={key}"
    return movie_name_base_url

def getMovieInfoBaseUrl():
    key = getkey()
    movie_info_base_url=f"http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json?key={key}"
    return movie_info_base_url

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

def chatbot(message):
    global global_command
    movie_name_base_url = getMovieNameBaseUrl()
    movie_info_base_url = getMovieInfoBaseUrl()

    apidict = {
        'movieNm': ['제목', '이름', '영화제목', '영화이름'],
        'genreNm': ['장르'],
        'directors_peopleNm': ['감독이름', '감독명', '감독'],
        'actors_peopleNm': ['배우이름', '배우명', '배우'],
        'openStartDt': ['개봉일', '개봉년도', '개봉연도']
    }

    bot_topic = message[0]
    bot_nic = message[1]
    bot_message = message[2]


    search_word = ''
    msglist = bot_message.split(' ')
    command = ''
    keyword = msglist[-1]
    movie_cd = ''

    # 검색어와 검색 키워드를 분리
    for msg in msglist:
        if msg != keyword:
            search_word = search_word + msg + " "

    for keys in apidict:
        for keywords in apidict[keys]:
            if keywords == keyword:
                command = keys
                url = movie_name_base_url + f"&movieNm={search_word.strip()}"
                movie_cd_r = requests.get(url)
                movie_cd_json = movie_cd_r.json()

                # api 호출 에러

                errcode = "320099"
                if movie_cd_json['faultInfo']['errorCode'] == errcode:
                    global_command['bot_nic'] = [f"{bot_nic}님, 현재 영화호출 api에 장애가 있습니다. 나중에 다시 시도해 주세요"]
                    send_message()

                if len(movie_cd_json['movieListResult']['movieList']) != 1:
                    movies_list = movie_cd_json['movieListResult']['movieList']
                    movies_cnt = movie_cd_json['movieListResult']['totCnt']
                    movie_cd_list = [movie['movieCd'] for movie in movies_list]
                    cnt = 1
                    movies_info_list = '\n'.join([f"{cnt + i}. 제목: {movie['movieNm']}, 장르: {movie['repGenreNm']}, 국가: {movie['repNationNm']}" for i, movie in enumerate(movies_list)])
                    global_command[f'bot_nic'] = [f"{bot_nic}님, 정확히 어떤 영화를 찾으시나요? 번호를 입력해주세요. ex) @bot 숫자\n\n {movies_info_list}", search_word, command, movie_cd_list]
                    
                    send_message()
                else:
                    movie_cd = movie_cd_json['movieListResult']['movieList'][0]['movieCd']
                    search_word = movie_cd_json['movieListResult']['movieList'][0]['movieNm']

    if len(movie_cd) > 0:
        info_url = movie_info_base_url + f"&movieCd={movie_cd}"
        info_r = requests.get(info_url)
        info_json = info_r.json()
        movie_info = info_json['movieInfoResult']['movieInfo']

        if command == 'genreNm':
            global_command[f'bot_nic'] = [f"{bot_nic}님, 영화 \"{search_word}\"의 장르는 {movie_info['genres'][0]['genreNm']}입니다."]
        elif command == 'directors_peopleNm':
            global_command[f'bot_nic'] = [f"{bot_nic}님, 영화 \"{search_word}\"의 감독은 {movie_info['directors'][0]['peopleNm']}입니다."]
        elif command == 'actors_peopleNm':
            actors = movie_info['actors']
            if len(actors) > 10:
                actors = actors[:5]
            actor_list = '\n'.join([f"{actor['peopleNm']} ({actor['cast']} 역)" for actor in actors])
            global_command[f'bot_nic'] = [f"{bot_nic}님, 영화 \"{search_word}\"의 배우는 {actor_list}입니다."]
        elif command == 'openStartDt':
            global_command[f'bot_nic'] = [f"{bot_nic}님, 영화 \"{search_word}\"의 개봉일은 {movie_info['openDt']}입니다."]
        elif command == 'movieNm':
            global_command[f'bot_nic'] = [f"{bot_nic}님, 영화의 제목은 \"{search_word}\"입니다."]
    else:
        global_command[f'bot_nic'] = ["{bot_nic}님, 해당 영화는 없습니다."]

    send_message()

def chatbotFindMovie(to_bot_data2):
    global global_command
    global bot_nic

    bot_topic = to_bot_data2[0]
    bot_nic = to_bot_data2[1]
    bot_message = to_bot_data2[2]
    real_movie_cd = to_bot_data2[3]
    command = to_bot_data2[4]

    find_movie_url = movie_info_base_url + f"&movieCd={real_movie_cd}"
    find_movie_r = requests.get(find_movie_url)
    find_movie_json = find_movie_r.json()

    find_movie = find_movie_json['movieInfoResult']['movieInfo']
    find_name = find_movie['movieNm']
    if command == 'genreNm':
        genres = find_movie['genres']
        genre_list = ', '.join([f"{genre['genreNm']}" for genre in genres])
        global_command[f'bot_nic'] = [f"{bot_nic}님, 영화 \"{find_name}\"의 장르는 {genre_list}입니다."]
    elif command == 'directors_peopleNm':
        directors = find_movie['directors']
        director_list = ', '.join([f"{director['peopleNm']}" for director in directors])
        global_command[f'bot_nic'] = [f"{bot_nic}님, 영화 \"{find_name}\"의 감독은 {director_list}입니다."]
    elif command == 'actors_peopleNm':
        actors = find_movie['actors']
        if len(actors) > 10:
            actors = actors[:5]
        actor_list = '\n'.join([f"{actor['peopleNm']} ({actor['cast']} 역)" for actor in actors])
        global_command[f'bot_nic'] = [f"{bot_nic}님, 영화 \"{search_word}\"의 배우는 {actor_list}입니다."]
    elif command == 'openStartDt':
        global_command[f'bot_nic'] = [f"{bot_nic}님, 영화 \"{search_word}\"의 개봉일은 {movie_info['openDt']}입니다."]
    elif command == 'movieNm':
        global_command[f'bot_nic'] = [f"{bot_nic}님, 영화의 제목은 \"{search_word}\"입니다."]
    else:
        global_command[f'bot_nic'] = ["{bot_nic}님, 해당 영화는 없습니다."]
    send_message()

def is_integer(s):
    try:
        int(s)
        return True
    except ValueError:
        return False

def send_message():
    global global_command
    global bot_nic
    server_address = import_ip()

    producer = KafkaProducer (
        bootstrap_servers=server_address,
        value_serializer=lambda x:json.dumps(x, ensure_ascii=False).encode('utf-8'),
        )


    while(True):
        if len(global_command) >= 1:
            for nic in list(global_command):
                message = global_command[nic][0]  # 챗봇 메시지 전송
                m_message = {'nickname': '@bot', 'message': message, 'time':time.strftime('%Y-%m-%d %H:%M:%S')}
                producer.send('team4', value=m_message)
                del global_command[nic]
                producer.flush()  # 메시지 전송 완료

def receive_message():
    global global_command
    chatroom = ['team4']
    server_address = import_ip()

    receiver = KafkaConsumer(
            bootstrap_servers=server_address,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            )
    receiver.subscribe(chatroom)
    print("Listener ready")
    print(f"Listen at {chatroom}, {server_address}")

    for message in receiver:
        data = message.value
        print(f"message receive : {data['message']}")
        if data['message'][:4] == "@bot":
            message = data['message'][5:]
            to_bot_data = [''.join(chatroom), data['nickname'], message]
            print(f"message receive : {to_bot_data}")
            chatbot(to_bot_data)

        elif data['message'][:4] == "@bot" and is_integer(data['message'][5:]):
            m, search_word, command, movie_cd_list = global_command
            real_movie_cd = movie_cd_list[int(data['message'][5:]) - 1]
            to_bot_data2 = [''.join(chatroom), data['nickname'], real_movie_cd, command]
            chatbotFindMovie(to_bot_data)

receive_message()
