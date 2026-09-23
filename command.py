import socket
import sys
import threading
import time
from pathlib import Path


CONTROL_IP = "127.0.0.1"
CONTROL_PORT = 9000

LOG_FILES = {
    "SERVER": "server.log",
    "P1": "process1.log",
    "P2": "process2.log",
    "P3": "process3.log",
}

def send_command(command: str):
    sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    try:
        sock.sendto(command.encode("utf-8"),(CONTROL_IP, CONTROL_PORT))

    finally:
        sock.close()


def follow_log(label: str, filename: str, stop_event: threading.Event):
    path = Path(filename)

    while not path.exists():
        if stop_event.is_set():
            return

        time.sleep(0.1)

    with path.open("r", encoding="utf-8", errors="replace") as log_file:
        log_file.seek(0, 2)

        while not stop_event.is_set():
            line = log_file.readline()
            if line:
                print(f"\n[{label}] {line.rstrip()}")

                print("controller> ", end="", flush=True)
            else:
                time.sleep(0.1)


def start_log_threads(stop_event: threading.Event):
    threads = []
    for label, filename in LOG_FILES.items():
        thread = threading.Thread(target=follow_log, args=(label,filename,stop_event),daemon=True)

        thread.start()

        threads.append(thread)

    return threads


def interactive_mode():
    stop_event = threading.Event()
    start_log_threads(stop_event)

    print("Distributed System Controller")
    print("Commands:")
    print("  send <process> <message>")
    print("  exit")
    print()

    try:
        while True:
            command = input("controller> ").strip()

            if not command:
                continue

            if command in {"exit", "quit"}:
                break

            send_command(command)

    except KeyboardInterrupt:
        print()

    finally:
        stop_event.set()


def single_command_mode():
    command = " ".join(sys.argv[1:])
    send_command(command)


def main():

    if len(sys.argv) > 1:
        single_command_mode()

    else:
        interactive_mode()

if __name__ == "__main__":
    main()