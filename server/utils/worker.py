import logging
import io

from utils.listener import Listener
from utils.model.message import Message, MessageType
from utils.middleware.middleware import ACK

TOTAL = "total"
WORKER_ID = "worker_id"


class ClientTracker():
    def __init__(self, client_id):
        self.client_id = client_id
        self.results = {}
        self.total_expected = -1
        self.total_worked = 0
        self.total_sent = 0

    def expect(self, expected):
        self.total_expected = expected

    def is_completed(self):
        return self.total_expected == self.total_worked

    def __repr__(self) -> str:
        n = len(self.results)
        return f'ClientTracker({n}, exp: {self.total_expected}, wrk: {self.total_worked}, snt: {self.total_sent})'

    def __str__(self) -> str:
        return self.__repr__()


class Worker(Listener):
    def __init__(self, middleware, in_serializer, out_serializer, peer_id, peers, chunk_size):
        super().__init__(middleware)
        self.peer_id = peer_id
        self.peers = peers
        self.chunk_size = chunk_size
        self.in_serializer = in_serializer
        self.out_serializer = out_serializer
        self.clients = {}
        self.tracker = None

    def forward_eof(self, eof):
        raise RuntimeError("Must be redefined")

    def forward_data(self, data):
        raise RuntimeError("Must be redefined")

    def work(self, input):
        return

    def do_after_work(self):
        return

    def send_chunk(self, chunk):
        logging.debug(f'action: send_results | status: in_progress | forwarding_chunk | len(chunk): {len(chunk)}')
        data = self.out_serializer.to_bytes(chunk)
        msg = Message(
            client_id=self.tracker.client_id,
            type=MessageType.DATA,
            data=data,
            args={
                WORKER_ID: self.peer_id,
            }
        )
        self.forward_data(msg.to_bytes())
        self.tracker.total_sent += len(chunk)
        return

    def send_results(self):
        chunk = []
        logging.debug(f'action: send_results | status: in_progress | len(results): {len(self.tracker.results)}')
        for result in self.tracker.results.values():
            chunk.append(result)
            if len(chunk) >= self.chunk_size:
                self.send_chunk(chunk)
                chunk = []
        if chunk:
            self.send_chunk(chunk)
        logging.debug('action: send_results | status: success')
        return

    def send_eof(self):
        eof = Message(
            client_id=self.tracker.client_id,
            type=MessageType.EOF,
            data=b'',
            args={
                TOTAL: self.tracker.total_sent,
                WORKER_ID: self.peer_id,
            }
        )
        self.forward_eof(eof.to_bytes())
        return

    def recv(self, raw_msg, key):
        msg = Message.from_bytes(raw_msg)
        if msg.client_id not in self.clients:
            self.clients[msg.client_id] = ClientTracker(msg.client_id)
        self.tracker = self.clients[msg.client_id]

        if msg.type == MessageType.EOF:
            self.recv_eof(msg.args[TOTAL])
        else:
            self.recv_raw(msg.data)

        return ACK

    def recv_raw(self, data):
        reader = io.BytesIO(data)
        input_chunk = self.in_serializer.from_chunk(reader)
        logging.debug(f'action: recv_raw | status: new_chunk | len(chunk): {len(input_chunk)}')
        for input in input_chunk:
            self.work(input)
        self.do_after_work()
        self.tracker.total_worked += len(input_chunk)

        if self.tracker.is_completed():
            self.send_results()
            self.send_eof()
        return

    def recv_eof(self, total):
        self.tracker.expect(total)
        if self.tracker.is_completed():
            self.send_results()
            self.send_eof()
        return
