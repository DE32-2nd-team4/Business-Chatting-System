from kafka import KafkaConsumer, KafkaProducer
import time
import json
import threading
import logging
import os
import requests
key = "82ca741a2844c5c180a208137bb92bd7"

# 영화 제목 모를때
movie_name_base_url=f"http://kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieList.json?key={key}"
# 영화 제목, 감독명, 배우명, 개봉일
movie_info_base_url=f"http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json?key={key}"

def chatbot(message):
    #print("메시지 분리합니다")
    #print(message)
    #print("\n *****************")
    apidict = {
        'movieNm': ['제목', '이름', '영화제목', '영화이름'],
        'genreNm': ['장르'],
        'directors_peopleNm': ['감독이름', '감독명', '감독'],
        'actors_peopleNm': ['배우이름', '배우명', '배우'],
        'openStartDt': ['개봉일', '개봉년도', '개봉연도']
    }

    search_word = ''
    msglist = message.split(' ')
    command = ''
    keyword = msglist[-1]
    movie_cd = ''

    # 검색어와 검색 키워드를 분리
    for msg in msglist:
        if msg != keyword:
            search_word = search_word + msg + " "
    #print("검색 키워드 입니다")
    #print(search_word)
    #print("\n ****************")

    for keys in apidict:
        for keywords in apidict[keys]:
            if keywords == keyword:
                command = keys
                #print("검색 커맨드 입니다")
                #print(command)
                #print("\n *********************")
                
                # 영화 이름으로 검색 후 영화 리스트 중에서 원하는 영화 선택, 영화 코드 찾기.
                url = movie_name_base_url + f"&movieNm={search_word.strip()}"
                #print("URL 입니다")
                #print(url)
                #print("\n *********************")
                movie_cd_r = requests.get(url)
                movie_cd_json = movie_cd_r.json()
                #print("json 입니다")
                #print(movie_cd_json)
                #print("\n ***********************************")

                if len(movie_cd_json['movieListResult']['movieList']) != 1:
                    movies_list = movie_cd_json['movieListResult']['movieList']
                    movies_cnt = movie_cd_json['movieListResult']['totCnt']
                    movie_cd_list = [movie['movieCd'] for movie in movies_list]
                    cnt = 1
                    movies_info_list = '\n'.join([f"{cnt + i}. 제목: {movie['movieNm']}, 장르: {movie['repGenreNm']}, 국가: {movie['repNationNm']}" for i, movie in enumerate(movies_list)])
                    return f"정확히 어떤 영화를 찾으시나요? 번호를 입력해주세요. ex) @bot 숫자 \n\n{movies_info_list}", search_word, command, movie_cd_list
                    
                else:
                    movie_cd = movie_cd_json['movieListResult']['movieList'][0]['movieCd']
                    search_word = movie_cd_json['movieListResult']['movieList'][0]['movieNm']

    if len(movie_cd) > 0:
        info_url = movie_info_base_url + f"&movieCd={movie_cd}"
        #print("info URL 입니다")
        #print(info_url)
        #print("\n *****************")
        info_r = requests.get(info_url)
        info_json = info_r.json()
        #print("info json 입니다")
        #print(info_json)
        #print("\n ******************************")

        movie_info = info_json['movieInfoResult']['movieInfo']

        #print("movie_info 입니다")
        #print(movie_info)
        #print("\n *********************************")

        if command == 'genreNm':
            return f"영화 \"{search_word}\"의 장르는 {movie_info['genres'][0]['genreNm']}입니다."
        elif command == 'directors_peopleNm':
            return f"영화 \"{search_word}\"의 감독은 {movie_info['directors'][0]['peopleNm']}입니다."
        elif command == 'actors_peopleNm':
            actors = movie_info['actors']
            if len(actors) > 10:
                actors = actors[:5]
            actor_list = '\n'.join([f"{actor['peopleNm']} ({actor['cast']} 역)" for actor in actors])
            return f"영화 \"{search_word}\"의 배우는 {actor_list}입니다."
        elif command == 'openStartDt':
            return f"영화 \"{search_word}\"의 개봉일은 {movie_info['openDt']}입니다."
        elif command == 'movieNm':
            return f"영화의 제목은 \"{search_word}\"입니다."
    else:
        return "해당 영화는 없습니다."

def chatbotFindMovie(real_movie_cd, command):
    find_movie_url = movie_info_base_url + f"&movieCd={real_movie_cd}"
    find_movie_r = requests.get(find_movie_url)
    find_movie_json = find_movie_r.json()
    
    find_movie = find_movie_json['movieInfoResult']['movieInfo']
    find_name = find_movie['movieNm']
    if command == 'genreNm':
        genres = find_movie['genres']
        genre_list = ', '.join([f"{genre['genreNm']}" for genre in genres])
        return f"영화 \"{find_name}\"의 장르는 {genre_list}입니다."
    elif command == 'directors_peopleNm':
        directors = find_movie['directors']
        director_list = ', '.join([f"{director['peopleNm']}" for director in directors])
        return f"영화 \"{find_name}\"의 감독은 {director_list}입니다."
    elif command == 'actors_peopleNm':
        actors = find_movie['actors']
        if len(actors) > 10:
            actors = actors[:5]
        actor_list = '\n'.join([f"{actor['peopleNm']} ({actor['cast']} 역)" for actor in actors])
        return f"영화 \"{search_word}\"의 배우는 {actor_list}입니다."
    elif command == 'openStartDt':
        return f"영화 \"{search_word}\"의 개봉일은 {movie_info['openDt']}입니다."
    elif command == 'movieNm':
        return f"영화의 제목은 \"{search_word}\"입니다."
    else:
        return "해당 영화는 없습니다."

def send_message():
    global username
    global chatroom
    
    
    log_file_path = f"~/atmp/chatdata/{chatroom}/chat.log"
    log_file_path = os.path.expanduser(log_file_path)
    # 로그 파일 경로 (EC2 서버의 team4 폴더 아래 chatroom 폴더)
    chatroom_dir = os.path.dirname(log_file_path)
    if not os.path.exists(chatroom_dir):
        os.makedirs(chatroom_dir)

# chat.log 파일 터치 (존재하지 않을 경우 생성)
    if not os.path.exists(log_file_path):
        open(log_file_path, 'a').close()  # 파일 생성
    producer = KafkaProducer (
        bootstrap_servers=['ec2-43-203-210-250.ap-northeast-2.compute.amazonaws.com:9092'],
        value_serializer=lambda x:json.dumps(x).encode('utf-8'),
        )


    while(True):
        message = input(" ")  # 사용자 입력 받기
        if message == "exit":
            producer.close()
            break
        if message[0:4] == "@bot":
            message = message[5:]
            message = chatbot(message)
            time.sleep(3)
            
            """
            if isinstance(message, tuple):
                m, search_word, command, movie_cd_list = message
                #print(m)
                #print(movie_cd_list)
                plus_message = input()
                if plus_message[0:4] == "@bot":
                    real_movie_cd = movie_cd_list[int(plus_message[5:]) - 1]
                    #print(real_movie_cd)
                    re_m = chatbotFindMovie(real_movie_cd, command)
                    time.sleep(3)
                    #print(re_m)
                    message = re_m
            else:
                #print(m)
                pass
            """
        m_message = {'nickname': username, 'message': message, 'time':time.time()}
        producer.send(chatroom, value=m_message)
        with open(log_file_path, 'a') as log_file:
            log_file.write(json.dumps(m_message) + '\n')  # 줄바꿈 추가
        # server/team4/chatroom/chat.log
        # m_message를 chat.log에 삽입
        producer.flush()  # 메시지 전송 완료



def receive_message():
    global chatroom
    global username
    receiver = KafkaConsumer(
            bootstrap_servers='ec2-43-203-210-250.ap-northeast-2.compute.amazonaws.com:9092',
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            )
    receiver.subscribe([chatroom])
    
    try:
        for message in receiver:
            data = message.value
            if username == data['nickname']:
                print(f"{data['time']}|{data['message']}")
            else:
                print(f"{data['nickname']} >> {data['message']} | {data['time']}")

    except KeyboardInterrput:
        print("채팅 종료")

    finally:
        receiver.close()


if __name__ == "__main__":
    print("채팅 프로그램 - 메시지 발신 및 수신")
    username = input("사용할 이름을 입력하세요 : ")
    chatroom = input("대화방 이름을 입력하세요 : ")

    consumer_thread = threading.Thread(target=receive_message)
    producer_thread = threading.Thread(target=send_message)

    consumer_thread.start()
    producer_thread.start()

