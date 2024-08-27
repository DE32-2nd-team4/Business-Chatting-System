import sys
import argparse

def main():
    parser = argparse.ArgumentParser(description='BCS 도움말', add_help=False)
    parser.add_argument('-h', '--help', action='store_true', help='도움말 표시')  # -h와 --help 옵션을 한 번만 정의
    parser.add_argument('-c', '--chat', action='store_true', help='src/bcs/main.py 실행')
    parser.add_argument('-a', '--audit', action='store_true', help='Zeppelin 실행 및 audit/data 폴더 열기')
    parser.add_argument('-i', '--ipconfig', action='store_true', help='config/ip 파일 열기')
    args = parser.parse_args()
    if args.help:
        print("Business Chatting System")  # 실제 도움 화면 내용으로 채워야 합니다.
        print()
        print("version 1.0")
        print()
        print()
        print()
        print()
        print("Commands")
        print("bcs, bcs -h, --help     : Show command help screen")
        print("         -c, --chat     : Enter chatting system")
        print("         -a, --audit    : Enter chat audit system")
        print("         -i, --ipconfig : Open config ip file")
        print()
        print()
        print()
        print()

    elif args.c:
        # src/bcs/main.py 실행 로직 추가
        subprocess.run(["python", "src/bcs/main.py"])
    elif args.a:
        # Zeppelin 실행 및 audit/data 폴더 열기 로직 추가
        print("Zeppelin 실행 및 audit/data 폴더 열기")
    elif args.i:
        # config/ip 파일 열기 로직 추가
        subprocess.run(["vim", "config/ip"])

if __name__ == '__main__':
    main()
