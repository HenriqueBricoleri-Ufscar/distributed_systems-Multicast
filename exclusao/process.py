import os
import sys
import socket
import threading
import importlib.util

# Reaproveita fifo.py, Message.py e process.py já existentes na raiz do repo,
# sem duplicar nada: só adiciona a raiz do projeto ao sys.path.
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT_DIR)

from Message import RequestMessage, ReplyMessage, TriggerMessage, Packet
from fifo import FifoChannel

# process.py da raiz tem o MESMO nome de módulo que este arquivo
# (exclusao/process.py), então ele é carregado por caminho explícito em vez
# de "from process import ...", evitando o self-import circular.
_spec = importlib.util.spec_from_file_location("multicast_process", os.path.join(ROOT_DIR, "process.py"))
_root_process = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_root_process)

PORT = _root_process.PORT
PEERS = _root_process.PEERS
INITIAL_CLOCKS = _root_process.INITIAL_CLOCKS
serialize_packet = _root_process.serialize_packet
deserialize_packet = _root_process.deserialize_packet

RELEASED, WANTED, HELD = "RELEASED", "WANTED", "HELD"


class MutexProcess:
    """Mesma infraestrutura de rede do process.py de multicast (socket UDP +
    FifoChannel + relógio de Lamport + PEERS/INITIAL_CLOCKS), trocando a
    lógica de entrega ordenada pela máquina de estados de Ricart & Agrawala."""

    def __init__(
        self,
        process_id: int,
        ip: str,
        port: int = PORT,
        peers: dict[str, tuple[str, int]] | None = None,
        transport=None,
    ):
        """transport, quando fornecido, é uma função transport(packet, dest_name)
        chamada no lugar do socket UDP real. Usado pelos testes para entregar
        pacotes de forma síncrona e determinística, sem abrir sockets nem threads
        de rede — a lógica do algoritmo (handle_packet) é exatamente a mesma
        usada em produção com Mininet."""
        self.process_id = process_id
        self.name = f"p{process_id}"
        self.ip = ip
        self.port = port
        self.peers = dict(PEERS if peers is None else peers)
        self.others = [name for name in self.peers if name != self.name]

        self.clock = INITIAL_CLOCKS[self.name]
        self.lock = threading.Lock()
        self.channel_lock = threading.Lock()

        self.state = RELEASED
        self.request_timestamp: int | None = None
        self.pending_replies: set[str] = set()
        self.deferred: list[str] = []
        self.cs_entered_event = threading.Event()

        self.transport = transport
        self.channel = None

        if transport is None:
            raw_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            raw_socket.bind((self.ip, self.port))
            self.channel = FifoChannel(raw_socket)

    def log(self, event: str, extra: str = ""):
        print(f"[{self.name}] {event} clock={self.clock} state={self.state}{extra}", flush=True)

    def send_packet(self, packet: Packet, dest_name: str):
        if self.transport is not None:
            self.transport(packet, dest_name)
        else:
            dest_ip, dest_port = self.peers[dest_name]
            data = serialize_packet(packet)
            with self.channel_lock:
                self.channel.sendto(data, (dest_ip, dest_port))

        kind = "REQUEST" if isinstance(packet, RequestMessage) else "REPLY"
        self.log(f"SEND {kind}", f" to={dest_name} timestamp={packet.timestamp}")

    def broadcast(self, packet: Packet):
        for name in self.others:
            self.send_packet(packet, name)

    # Pedido / liberação da CS, disparados por TriggerMessage do servidor
    def request_cs(self):
        with self.lock:
            self.clock += 1
            self.state = WANTED
            self.request_timestamp = self.clock
            self.pending_replies = set(self.others)
            packet = RequestMessage(self.request_timestamp, self.name)

        self.log("REQUEST CS", f" timestamp={self.request_timestamp}")
        self.broadcast(packet)

        self.cs_entered_event.wait()
        self.cs_entered_event.clear()
        self.log("HOLDING CS", " (aguardando comando 'release')")

    def release_cs(self):
        with self.lock:
            if self.state != HELD:
                self.log("RELEASE IGNORED", " (não está em HELD)")
                return
            self.state = RELEASED
            deferred, self.deferred = self.deferred, []
            self.request_timestamp = None

        self.log("EXIT CS")

        for name in deferred:
            with self.lock:
                self.clock += 1
                clock = self.clock
            self.send_packet(ReplyMessage(clock, self.name), name)

    # Recepção de pacotes
    def handle_packet(self, packet: Packet, source: tuple[str, int]):
        reply_to_send = None
        entered_cs = False

        with self.lock:
            self.clock = max(self.clock, packet.timestamp) + 1

            if isinstance(packet, RequestMessage):
                self.log("RECEIVE REQUEST", f" from={packet.sender} timestamp={packet.timestamp}")

                i_have_priority = (
                    self.state == WANTED
                    and (self.request_timestamp, self.name) < (packet.timestamp, packet.sender)
                )

                if self.state == HELD or i_have_priority:
                    self.deferred.append(packet.sender)
                    self.log("DEFER REPLY", f" to={packet.sender}")
                else:
                    self.clock += 1
                    reply_to_send = (ReplyMessage(self.clock, self.name), packet.sender)

            elif isinstance(packet, ReplyMessage):
                self.log("RECEIVE REPLY", f" from={packet.sender}")
                self.pending_replies.discard(packet.sender)

                if self.state == WANTED and not self.pending_replies:
                    self.state = HELD
                    entered_cs = True

            elif isinstance(packet, TriggerMessage):
                pass  # tratado fora do lock

            else:
                raise TypeError(f"Unsupported packet: {packet}")

        if reply_to_send is not None:
            reply, dest = reply_to_send
            self.send_packet(reply, dest)

        if entered_cs:
            self.log("ENTER CS")
            self.cs_entered_event.set()

        if isinstance(packet, TriggerMessage):
            if packet.action == "REQUEST":
                threading.Thread(target=self.request_cs, daemon=True).start()
            elif packet.action == "RELEASE":
                threading.Thread(target=self.release_cs, daemon=True).start()
            else:
                self.log("UNKNOWN TRIGGER", f" action={packet.action}")

    def run(self):
        if self.channel is None:
            raise RuntimeError("run() requer socket real; não use com transport customizado (testes)")
        print(f"[{self.name}] running on {self.ip}:{self.port} clock={self.clock}", flush=True)
        while True:
            data, source = self.channel.recvfrom(65535)
            packet = deserialize_packet(data)
            self.handle_packet(packet, source)


def main():
    if len(sys.argv) != 3:
        print("Usage: python3 exclusao/process.py <process_id> <ip>")
        sys.exit(1)

    process_id = int(sys.argv[1])
    ip = sys.argv[2]
    process = MutexProcess(process_id=process_id, ip=ip)
    process.run()


if __name__ == "__main__":
    main()