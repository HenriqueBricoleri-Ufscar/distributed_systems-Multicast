import json
from collections import deque


class FifoChannel:
    def __init__(self, sock):
        self.socket = sock
        self.sent = {}
        self.expected = {}
        self.pending = {}
        self.ready = deque()

    def sendto(self, data: bytes, destination: tuple[str, int]):
        sequence = self.sent.get(destination, 0) + 1
        frame = json.loads(data.decode("utf-8"))
        frame["sequence"] = sequence
        self.socket.sendto(json.dumps(frame).encode("utf-8"), destination)
        self.sent[destination] = sequence

    def recvfrom(self, size: int) -> tuple[bytes, tuple[str, int]]:
        while not self.ready:
            data, source = self.socket.recvfrom(size)
            frame = json.loads(data.decode("utf-8"))
            sequence = frame.pop("sequence", None)

            if sequence is None:
                return data, source

            expected = self.expected.get(source, 1)
            if sequence < expected:
                continue

            pending = self.pending.setdefault(source, {})
            pending.setdefault(sequence, json.dumps(frame).encode("utf-8"))

            if sequence > expected:
                print(f"[FIFO] BUFFER source={source[0]}:{source[1]} sequence={sequence} expected={expected}", flush=True)

            while expected in pending:
                self.ready.append((pending.pop(expected), source))
                expected += 1

            self.expected[source] = expected
            if not pending:
                del self.pending[source]

        return self.ready.popleft()
