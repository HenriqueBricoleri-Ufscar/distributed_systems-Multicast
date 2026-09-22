import socket
import threading


SERVER_IP = "10.0.0.1"
SERVER_PORT = 5000

CONTROL_IP = "127.0.0.1"
CONTROL_PORT = 9000


processes = {
    "p1": ("10.0.0.2", 5000),
    "p2": ("10.0.0.3", 5000),
    "p3": ("10.0.0.4", 5000)
}


server_socket = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)

server_socket.bind((SERVER_IP, SERVER_PORT))


def send_message(process_id, message):
    dest_proc = processes.get(process_id)

    if dest_proc is None:
        raise KeyError(f"process '{process_id}' not found!")

    server_socket.sendto(message.encode("utf-8"), dest_proc)

    print(f"[SEND] {dest_proc} - {message}")


def process_ack():
    while True:
        data, source_ip = server_socket.recvfrom(1024)

        if data == b"":
            print(f"[ACK] received from {source_ip}")


def server_controller():
    control_socket = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)

    control_socket.bind((CONTROL_IP, CONTROL_PORT))

    print(f"[CONTROLLER] listening on: " 
          f"{CONTROL_IP}:{CONTROL_PORT}")

    while True:
        data, addr = control_socket.recvfrom(4096)

        command = data.decode("utf-8")

        print(f"[CONTROLLER] command received: {command}")

        parts = command.split(maxsplit=2)

        if not parts:
            continue

        if parts[0] == "send":

            if len(parts) != 3:
                print("Send message is not in the right format. Use: send <process> <message>")
                continue

            _, process_name, message = parts

            try:
                send_message(process_name, message)

            except KeyError as e:
                print(f"Routing Error: {e}")

        else:
            print(f"Command NOT FOUND: "
                  f"{parts[0]}")


def main():
    print(f"[SERVER] UDP socket running on: "
          f"{SERVER_IP}:{SERVER_PORT}")

    thread_ack = threading.Thread(target=process_ack, daemon=True)

    thread_control = threading.Thread(target=server_controller, daemon=True)

    thread_ack.start()
    thread_control.start()

    thread_ack.join()


if __name__ == "__main__":
    main()