import curses

def main(stdscr):
    # 터미널 화면을 클리어
    #stdscr.clear()

    # 입력을 받을 창을 생성
    input_win = curses.newwin(1, curses.COLS, curses.LINES - 1, 0)
#    output_win = curses.newwin(curses.LINES - 1, curses.COLS, 0, 0)

    # 출력 창에 텍스트를 출력
#    output_win.addstr(0, 0, "이제 입력을 기다립니다:")
#    output_win.refresh()

    while True:
        # 입력 창에 커서 위치
        input_win.addstr(0, 0, "입력: ")
        input_win.refresh()
        
        # 입력 받기
        curses.curs_set(1)  # 커서 보이기
        curses.echo()
        user_input = input_win.getstr().decode('utf-8')  # 입력 받기
        curses.noecho()  # 입력된 문자가 화면에 보이지 않도록 설정
        curses.curs_set(0)  # 커서 숨기기
        input_win.clear()

        if user_input.lower() == 'exit':
            break
        
        # 입력 결과를 출력 창에 추가
#        output_win.addstr(1, 0, f"입력한 값: {user_input}")
#        output_win.refresh()

curses.wrapper(main)

