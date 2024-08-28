import sys
import argparse
import subprocess

def main():
    parser = argparse.ArgumentParser(description='BCS 도움말', add_help=False)
    parser.add_argument('-h', '--help', action='store_true', help='도움말 표시')  # -h와 --help 옵션을 한 번만 정의
    parser.add_argument('-c', '--chat', action='store_true', help='src/bcs/main.py 실행')
    parser.add_argument('-a', '--audit', action='store_true', help='Zeppelin 실행 및 audit/data 폴더 열기')
    parser.add_argument('-i', '--ipconfig', action='store_true', help='config/ip 파일 열기')
    parser.add_argument('-b', '--bot', action='store_true', help='모든 봇 시작')
    args = parser.parse_args()
    if args.help:
        print("""
        Business Chatting System
        Version 1.0

        This repository is a package that combines business chat, chat auditing, and chatbot systems. 
        It allows you to communicate securely with colleagues in other departments in security-sensitive work environments, 
        The versatile chatbots included can boost productivity, 
        and the system can quickly detect any potential threats or conspiracies against them. 
        All of this is possible simply by utilizing this repository.
        
        
        
        Commands
        bcs, bcs -h, --help     : Show command help screen
                 -c, --chat     : Enter chatting system
                 -a, --audit    : Enter chat audit system
                 -i, --ipconfig : Open config ip file
                 -b, --bot      : Start bot
        
        
        
        
        """)
    elif args.chat:
        # src/bcs/main.py 실행 로직 추가
        subprocess.run(["python", "src/bcs/main.py"])
    elif args.audit:
        # Zeppelin 실행 및 audit/data 폴더 열기 로직 추가
        print("Zeppelin 실행 및 audit/data 폴더 열기")
    elif args.ipconfig:
        # config/ip 파일 열기 로직 추가
        subprocess.run(["vim", "config/ip"])
    elif args.bot:
        # bot 실행
        processes = []
        for bot_file in ["src/bcs/system_bot.py", "src/bcs/aleam_bot.py", "src/bcs/movie_bot.py"]:
            process = subprocess.Popen(
                    ["python", bot_file],
                    stdout=devnull,  # 표준 출력을 닫기
                    stderr=devnull,  # 표준 오류 출력을 닫기
                    stdin=devnull,   # 표준 입력을 닫기
                    close_fds=True,  # 부모 프로세스의 파일 디스크립터를 닫기
                    preexec_fn=os.setsid  # 새로운 세션을 생성하여 독립적으로 실행되도록 설정
                    )
            processes.append(process)
            print("bot 실행 완료")

        # 필요하다면, 모든 백그라운드 프로세스가 종료될 때까지 기다릴 수 있습니다.
        for process in processes:
            process.wait() 

if __name__ == '__main__':
    main()
