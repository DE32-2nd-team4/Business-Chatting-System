# src/chat.py
from kafka import KafkaProducer, KafkaConsumer
from json import loads, dumps
import threading
import curses


def resize_win(stdscr, lowerwin_height=3):
    height, width = stdscr.getmaxyx()
    lowerwin_height = 3
    lowerwin_y = height - lowerwin_height
    lowerwin_x = 0

    # windows
    upperwin = stdscr.subwin(height - lowerwin_height, width, 0, 0)
    lowerwin = stdscr.subwin(lowerwin_height, width, lowerwin_y, lowerwin_x)

    # enable scrolling
    upperwin.scrollok(True)
    lowerwin.scrollok(True)
    
    return lowerwin, upperwin

def create_data(username, message, end):
    return {'sender': username, 'message': message, 'end': end}

def pchat(chatroom, username, lowerwin, upperwin, lock):
    sender = KafkaProducer(
        bootstrap_servers = ['localhost:9092'],
        # bootstrap_servers = ['ec2-43-203-182-252.ap-northeast-2.compute.amazonaws.com:9092'],
        value_serializer = lambda x: dumps(x).encode('utf-8'),
    )

    try:
        data = create_data("[INFO]", f"User [{username}] has entered the chat!\n", False)
        end = False
        sender.send(chatroom, data)
        sender.flush()        

        while True:
            with lock:
                lowerwin.clear()
                lowerwin.addstr(0, 0, f"YOU: ")
                lowerwin.refresh()

            message = lowerwin.getstr(0, len(f"YOU: "), 70).decode('utf-8')

            if message == "":
                continue

            if message == 'exit':
                message = f"\nUser [{username}] has exited the chatroom."
                user = '[INFO]'
                end = True

            data = {'sender': username, 'message': message, 'end': end}       
            sender.send(chatroom, value=data)
            sender.flush()

            if end:
                return    

    except KeyboardInterrupt:
        upperwin.addstr("[INFO] Encountered keyboard interrupt. Finishing chat...")
        upperwin.refresh()
        return

    
# RECEIVER
def cchat(chatroom, username, lowerwin, upperwin, lock):
    receiver = KafkaConsumer(
        chatroom,
        bootstrap_servers = ['localhost:9092'],
        # bootstrap_servers = ['ec2-43-203-182-252.ap-northeast-2.compute.amazonaws.com:9092'],
        enable_auto_commit = True,
        value_deserializer = lambda x: loads(x.decode('utf-8'))
    )

    try:
        for message in receiver:
            data = message.value

            with lock:
                if data['end'] == True:
                    if data['sender'] != username:
                        upperwin.addstr(f"\n{data['message']}\n")
                        upperwin.addstr("Type in 'exit' to also finish the chat.\n")
                        upperwin.refresh()

                    else:
                        lowerwin.clear()
                        lowerwin.refresh()
                        lowerwin.addstr(f"\nExiting chat...\n")
                        lowerwin.addstr(f"Presss any key to exit.")
                        lowerwin.getch()
                        return

                else:
                    upperwin.addstr(f"{data['sender']}: {data['message']}\n")
                    upperwin.refresh()

            if upperwin.getyx()[0] >= upperwin.getmaxyx()[0] -1:
                with lock:
                    upperwin.scroll(1)
                    upperwin.refresh()

    except KeyboardInterrupt:
        upperwin.refresh()("Exiting chat...")
        return


lock = threading.Lock()

stdscr = curses.initscr() 
stdscr.keypad(True)
lowerwin, upperwin = resize_win(stdscr, lowerwin_height=3)
stdscr.clear()

lowerwin.addstr(0, 0, "Input chatroom name: ")
chatroom = lowerwin.getstr(0, 20, 70).decode('utf-8')
lowerwin.addstr(1, 0, "Input username: ")
username = lowerwin.getstr(1, 20, 70).decode('utf-8')

with lock:
    upperwin.addstr(0, 0, f"[INFO] Initializing chatroom [{chatroom}] for user [{username}]...\n")
    upperwin.addstr(1, 0, "[INFO] Initialize complete! Enjoy chatting :)\n\n")
    upperwin.refresh()

    lowerwin.clear()
    lowerwin.refresh()

thread_1 = threading.Thread(target = pchat, args = (chatroom, username, lowerwin, upperwin, lock))
thread_2 = threading.Thread(target = cchat, args = (chatroom, username, lowerwin, upperwin, lock))

thread_1.start()
thread_2.start()

thread_1.join()
thread_2.join()

curses.endwin()
