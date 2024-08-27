import sys
import argparse

def main():
    parser = argparse.ArgumentParser(description='BCS 도움말')
    parser.add_argument('-h', '--help', action='store_true', help='도움말 표시')
    parser.add_argument('-c', action='store_true', help='src/bcs/main.py 실행')
    parser.add_argument('-a', action='store_true', help='Zeppelin 실행 및 audit/data 폴더 열기')
    parser.add_argument('-i', action='store_true', help='config/ip 파일 열기')
    args = parser.parse_args()

    if args.help:
        print("도움 화면 내용")  # 실제 도움 화면 내용으로 채워야 합니다.
    elif args.c:
        # src/bcs/main.py 실행 로직 추가
        print("src/bcs/main.py 실행")
    elif args.a:
        # Zeppelin 실행 및 audit/data 폴더 열기 로직 추가
        print("Zeppelin 실행 및 audit/data 폴더 열기")
    elif args.i:
        # config/ip 파일 열기 로직 추가
        print("config/ip 파일 열기")

if __name__ == '__main__':
    main()
