import curses

def main(stdscr):
    # 터미널 화면을 클리어
#    stdscr.clear()

    # 입력을 받을 창을 생성
    input_win = curses.newwin(1, curses.COLS, curses.LINES - 1, 0)
#    output_win = curses.newwin(curses.LINES - 1, curses.COLS, 0, 0)

    # 출력 창에 텍스트를 출력
#    output_win.addstr(0, 0, "이제 입력을 기다립니다:")
#    output_win.refresh()

    # 입력을 받을 버퍼
    input_buffer = ""
    
    while True:
        # 입력 창에 커서 위치
        input_win.clear()
        input_win.addstr(0, 0, "입력: " + input_buffer)
        input_win.refresh()

        # 입력 받기
        key = input_win.getch()
        
        # 종료 조건: 'q' 키를 누르면 종료
        if key == ord('q'):
            break
        
        # 백스페이스 처리
        elif key == curses.KEY_BACKSPACE or key == 127:
            input_buffer = input_buffer[:-1]

        # 입력된 키가 문자일 때
        elif 32 <= key <= 126:  # ASCII printable characters
            input_buffer += chr(key)

        # 입력 결과를 출력 창에 추가
     #   output_win.clear()
      #  output_win.addstr(1, 0, f"입력한 값: {input_buffer}")
 #       output_win.refresh()
#
curses.wrapper(main)
