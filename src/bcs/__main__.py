import sys
import argparse
import subprocess

def main():
    parser = argparse.ArgumentParser(description='BCS 도움말', add_help=False)
    parser.add_argument('-h', '--help', action='store_true', help='도움말 표시')  # -h와 --help 옵션을 한 번만 정의
    parser.add_argument('-c', '--chat', action='store_true', help='src/bcs/main.py 실행')
    parser.add_argument('-a', '--audit', action='store_true', help='Zeppelin 실행 및 audit/data 폴더 열기')
    parser.add_argument('-i', '--ipconfig', action='store_true', help='config/ip 파일 열기')
    parser.add_augument('-b', '--bot', action='store_true', help='모든 봇 시작')
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
        subprocess.run(["python", "src/bcs/system_bot.py"])
        subprocess.run(["python", "src/bcs/aleam_bot.py"])
        subprocess.run(["python", "src/bcs/movie_bot.py"])


if __name__ == '__main__':
    main()
