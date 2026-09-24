import socket, sys, json
from dataclasses import asdict

from Message import DataMessage, AckMessage, RequestMessage, ReplyMessage, TriggerMessage, Packet
from fifo import FifoChannel

PORT = 5000
PEERS = {
    "p1": ("10.0.0.2", PORT),
    "p2": ("10.0.0.3", PORT),
    "p3": ("10.0.0.4", PORT)
}
INITIAL_CLOCKS = {"p1": 0, "p2": 5, "p3": 10}

def serialize_packet(packet: Packet) -> bytes:
    data = asdict(packet)

    if isinstance(packet, DataMessage):
        data["type"] = "DATA"

    elif isinstance(packet, AckMessage):
        data["type"] = "ACK"

    # mensagens para o algoritmo de ricart e agrawala
    elif isinstance(packet, RequestMessage):
        data["type"] = "REQUEST"

    elif isinstance(packet, ReplyMessage):
        data["type"] = "REPLY"

    elif isinstance(packet, TriggerMessage):
        data["type"] = "TRIGGER"

    else:
        raise TypeError(f"Unsupported packet type: {type(packet)}")

    return json.dumps(data).encode("utf-8")

def deserialize_packet(data: bytes) -> Packet:
    packet_data = json.loads(data.decode("utf-8"))

    packet_type = packet_data.pop("type")

    if packet_type == "DATA":
        return DataMessage(**packet_data)

    if packet_type == "ACK":
        return AckMessage(**packet_data)

    # mensagens para o algoritmo de ricart e agrawala
    if packet_type == "REQUEST": 
        return RequestMessage(**packet_data)

    if packet_type == "REPLY":
        return ReplyMessage(**packet_data)

    if packet_type == "TRIGGER":
        return TriggerMessage(**packet_data)

    raise ValueError(f"Unknown packet type: {packet_type}")

class Process:
    def __init__(self, process_id: int, ip: str, port: int = PORT, peers: dict[str, tuple[str, int]] | None = None):
        self.process_id = process_id
        self.name = f"p{process_id}"
        self.ip = ip
        self.port = port
        self.peers = dict(PEERS if peers is None else peers)
        self.clock = INITIAL_CLOCKS[self.name]
        self.sequence = 0
        self.queue: list[DataMessage] = []
        self.acknowledgments: dict[str, set[str]] = {}
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind((self.ip, self.port))
        self.channel = FifoChannel(self.socket)

    def log_packet(self, event: str, packet: Packet, endpoint: str = ""):
        packet_type = "DATA" if isinstance(packet, DataMessage) else "ACK"
        content = f" content={packet.content!r}" if isinstance(packet, DataMessage) else ""
        print(f"[{self.name}] {event} clock={self.clock} type={packet_type} id={packet.msg_id} timestamp={packet.timestamp} sender={packet.sender}{endpoint}{content}", flush=True)

    def send_packet(self, packet: Packet, dest_ip: str, dest_port: int = PORT):
        data = serialize_packet(packet)
        dest = (dest_ip, dest_port)
        self.channel.sendto(data, dest)
        self.log_packet("SEND", packet, f" destination={dest_ip}:{dest_port}")

    def receive_packet(self) -> tuple[Packet, tuple[str, int]]:
        data, source = self.channel.recvfrom(65535)
        return deserialize_packet(data), source

    def multicast_packet(self, packet: Packet):
        for ip, port in self.peers.values():
            self.send_packet(packet, ip, port)

    def multicast(self, content: str):
        self.clock += 1
        self.sequence += 1
        packet = DataMessage(f"{self.name}-{self.sequence}", self.clock, self.name, content)
        self.multicast_packet(packet)

    def handle_packet(self, packet: Packet, source: tuple[str, int]):
        self.clock = max(self.clock, packet.timestamp) + 1
        self.log_packet("RECEIVE", packet, f" source={source[0]}:{source[1]}")

        if isinstance(packet, DataMessage) and packet.sender == "server":
            self.multicast(packet.content)
            return

        if isinstance(packet, DataMessage):
            self.queue.append(packet)
            self.queue.sort(key=lambda message: (message.timestamp, int(message.sender[1:])))
            self.acknowledgments.setdefault(packet.msg_id, set())
            self.clock += 1
            self.multicast_packet(AckMessage(packet.msg_id, self.clock, self.name))

        elif isinstance(packet, AckMessage):
            self.acknowledgments.setdefault(packet.msg_id, set()).add(packet.sender)

        self.deliver_messages()

    def deliver_messages(self):
        while self.queue:
            packet = self.queue[0]
            if not self.acknowledgments[packet.msg_id].issuperset(self.peers):
                break
            self.queue.pop(0)
            del self.acknowledgments[packet.msg_id]
            self.clock += 1
            self.deliver_message(packet)

    def deliver_message(self, packet: DataMessage):
        self.log_packet("DELIVER", packet)

    def run(self):
        print(f"[{self.name}] running on {self.ip}:{self.port} clock={self.clock}", flush=True)
        while True:
            packet, source = self.receive_packet()
            self.handle_packet(packet, source)

def main():
    if len(sys.argv) != 3:
        print("Usage: python3 process.py <process_id> <ip>")
        sys.exit(1)

    process_id = int(sys.argv[1])
    ip = sys.argv[2]
    process = Process(process_id=process_id, ip=ip)
    process.run()

if __name__ == "__main__":
    main()