import socket, sys, json
from dataclasses import asdict

from Message import *

PORT = 5000

def serialize_packet(packet: Packet) -> bytes:
    data = asdict(packet)

    if isinstance(packet, DataMessage):
        data["type"] = "DATA"

    elif isinstance(packet, AckMessage):
        data["type"] = "ACK"

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
    
    raise ValueError(f"Unknown packet type: {packet_type}")

class Process: 
    def __init__(self, process_id: int, ip: str, port: int = PORT):
        self.process_id = process_id
        self.name = f"p{process_id}"
        
        self.ip = ip
        self.port = port
        
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind((self.ip, self.port))
        
    def send_packet(self, packet: Packet, dest_ip: str, dest_port: int = PORT):
        data = serialize_packet(packet)
        dest = (dest_ip, dest_port)
        
        self.socket.sendto(data, dest)
        print(f"[{self.name}] SEND -> {dest}: {packet}")
        
    def receive_packet(self) -> tuple[Packet, tuple[str, int]]:
        data, source = self.socket.recvfrom(4096)
        
        packet = deserialize_packet(data)
        print(f"[{self.name}] RECEIVE <- {source}: {packet}")

        return packet, source
    
    def handle_packet(self, packet: Packet, source: tuple[str, int]):
        if isinstance(packet, DataMessage):
            print(
                f"[{self.name}] DATA MESSAGE\n"
                f"  id:        {packet.msg_id}\n"
                f"  sender:    {packet.sender}\n"
                f"  timestamp: {packet.timestamp}\n"
                f"  content:   {packet.content}"
            )

        elif isinstance(packet, AckMessage):
            print(
                f"[{self.name}] ACK MESSAGE\n"
                f"  id:        {packet.msg_id}\n"
                f"  sender:    {packet.sender}\n"
                f"  timestamp: {packet.timestamp}"
            )
            
    def run(self):
        print(
            f"[{self.name}] running on "
            f"{self.ip}:{self.port}"
        )
    
        while True:
            packet, source = self.receive_packet()
            self.handle_packet(packet, source)
        
def main():

    if len(sys.argv) != 3:
        print(
            "Usage:"
        )
        print(
            "python3 process.py "
            "<process_id> <ip>"
        )
        sys.exit(1)

    process_id = int(
        sys.argv[1]
    )

    ip = sys.argv[2]

    process = Process(
        process_id=process_id,
        ip=ip
    )

    process.run()


if __name__ == "__main__":
    main()
        
         
        

