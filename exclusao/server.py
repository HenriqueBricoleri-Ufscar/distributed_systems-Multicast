import os
import sys
import socket
import threading
import json
import importlib.util
from dataclasses import asdict

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT_DIR)

from Message import TriggerMessage

# server.py da raiz tem o mesmo nome de módulo deste arquivo (exclusao/server.py),
# então é carregado por caminho explícito, evitando o self-import circular.
_spec = importlib.util.spec_from_file_location("multicast_server", os.path.join(ROOT_DIR, "server.py"))
_root_server = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_root_server)

# Reaproveitados sem redefinir: endereços, portas e tabela de processos
SERVER_IP = _root_server.SERVER_IP
SERVER_PORT = _root_server.SERVER_PORT
CONTROL_IP = _root_server.CONTROL_IP
CONTROL_PORT = _root_server.CONTROL_PORT
processes = _root_server.processes

server_socket: socket.socket


def trigger(process_id: str, action: str):
    dest = processes.get(process_id)

    if dest is None:
        raise KeyError(f"process '{process_id}' not found!")

    packet = TriggerMessage(action=action, timestamp=0, sender="server")
    data = asdict(packet)
    data["type"] = "TRIGGER"

    server_socket.sendto(json.dumps(data).encode("utf-8"), dest)
    print(f"[SEND] {dest} - {action}")


def server_controller():
    control_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    control_socket.bind((CONTROL_IP, CONTROL_PORT))

    print(f"[CONTROLLER] listening on: {CONTROL_IP}:{CONTROL_PORT}")
    print("Commands: request <processo> | release <processo>")

    while True:
        data, addr = control_socket.recvfrom(4096)
        command = data.decode("utf-8")
        print(f"[CONTROLLER] command received: {command}")

        parts = command.split()

        if not parts:
            continue

        if parts[0] in ("request", "release") and len(parts) == 2:
            action = "REQUEST" if parts[0] == "request" else "RELEASE"

            try:
                trigger(parts[1], action)
            except KeyError as e:
                print(f"Routing Error: {e}")
        else:
            print(f"Command NOT FOUND or malformed: {command}. Use: request <p> | release <p>")


def main():
    global server_socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_socket.bind((SERVER_IP, SERVER_PORT))

    print(f"[SERVER] UDP socket running on: {SERVER_IP}:{SERVER_PORT}")

    thread_control = threading.Thread(target=server_controller, daemon=True)
    thread_control.start()
    thread_control.join()


if __name__ == "__main__":
    main()