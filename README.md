# DE32-2rd_team4
# Businness Chatting System
<Need add Picture>

## Introduce
<Need add Introduce text>
이 레포지토리는 업무용 채팅, 채팅 감사기능, 챗봇 시스템 등이 결합된 패키지입니다.
보안이 필요한 업무에서 안전하게 다른 부서 직원들과 소통할 수 있고,
그들에게 다양한 기능을 함께하는 챗봇들이 일의 능률을 올려주며,
그들에게 뻗치는 음모를 빠르게 파악 할 수 있습니다.
단지 이 레포지토리를 이용하는 것 만으로요.


## Installation
### For Users
<Need add how to install for user>
사용자 분들은 본 레포지토리를 사용하기 위해 몇가지 준비가 필요합니다.
먼저, 본 레포지토리를 다운로드 해주십시오.
```
git clone https://github.com/DE32-2nd-team4/Business-Chatting-System.git
```

레포지토리를 설치 한 다음 몇 가지 초기 설정과 설치가 필요합니다.

### Airflow

https://airflow.apache.org/docs/apache-airflow/stable/start.html
```
$ pwd
/<your home>

$ vi .zshrc  # zsh

export AIRFLOW_HOME=~/<Your Install Location>
export AIRFLOW__CORE__DAGS_FOLDER=~/<Your Install Location>/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
```

### Kafka

https://kafka.apache.org/quickstart
```
$ pwd
/<your Kafka install location>

$ vi config/server.properties
advertised.listeners=PLAINTEXT://ec2-43-203-210-250.ap-northeast-2.compute.amazonaws.com:9092
or
advertised.listeners=<Your Server IP>
```

# 

위의 과정들이 모두 마무리되었다면, 본 레포지토리 설치를 진행해 주십시오.
```
$ git clone https://github.com/DE32-2nd-team4/Business-Chatting-System.git
$ cd Business-Chatting-System
$ source .venv/bin/activate
```

### For Dev
<Need add how to install for devs>

## Usage

### Chat System
```
<Need add command how to use chat system>
```
<Need add usage picture>


### Chat Audit System
```
<Need add command how to use chat audit system>
```
<Need add usage picture>

### Chat-bot System
```
<Need add command how to use chat-bot system>
```
<Need add usage picture>
-

## Reference
<Need add Reference List>

