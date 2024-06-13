import logging
import io

from utils.listener import Listener
from utils.model.message import Message, MessageType
from utils.middleware.middleware import ACK, NACK

TOTAL = "total"
WORKER_ID = "worker_id"


class Worker(Listener):
    def __init__(self, middleware, in_serializer, out_serializer, peer_id, peers, chunk_size):
        super().__init__(middleware)
        self.peer_id = peer_id
        self.peers = peers
        self.chunk_size = chunk_size
        self.results = {}
        self.in_serializer = in_serializer
        self.out_serializer = out_serializer
        self.total_expected = -1
        self.total_worked = 0
        self.total_sent = 0

    def forward_eof(self, eof):
        raise RuntimeError("Must be redefined")

    def forward_data(self, data):
        raise RuntimeError("Must be redefined")

    def work(self, input, client_id):
        return

    def do_after_work(self, client_id):
        return

    def send_chunk(self, chunk, client_id):
        logging.debug(f'action: send_results | status: in_progress | forwarding_chunk | len(chunk): {len(chunk)}')
        data = self.out_serializer.to_bytes(chunk)
        msg = Message(
            client_id=client_id,
            type=MessageType.DATA,
            data=data,
            args={
                WORKER_ID: self.peer_id,
            }
        )
        self.forward_data(msg.to_bytes())
        self.total_sent += len(chunk)
        return

    def send_results(self, client_id):
        chunk = []
        logging.debug(f'action: send_results | status: in_progress | len(results): {len(self.results)}')
        for result in self.results.values():
            chunk.append(result)
            if len(chunk) >= self.chunk_size:
                self.send_chunk(chunk, client_id)
                chunk = []
        if chunk:
            self.send_chunk(chunk, client_id)
        logging.debug('action: send_results | status: success')
        return

    def send_eof(self, client_id):
        eof = Message(
            client_id=client_id,
            type=MessageType.EOF,
            data=b'',
            args={
                TOTAL: self.total_sent,
                WORKER_ID: self.peer_id,
            }
        )
        self.forward_eof(eof.to_bytes())
        return

    def recv(self, raw_msg, key):
        msg = Message.from_bytes(raw_msg)
        if msg.type == MessageType.EOF:
            self.recv_eof(msg.args[TOTAL], msg.client_id)
        else:
            self.recv_raw(msg.data, msg.client_id)

        return ACK

    def recv_raw(self, data, client_id):
        reader = io.BytesIO(data)
        input_chunk = self.in_serializer.from_chunk(reader)
        logging.debug(f'action: recv_raw | status: new_chunk | len(chunk): {len(input_chunk)}')
        for input in input_chunk:
            self.work(input, client_id)
        self.do_after_work(client_id)
        self.total_worked += len(input_chunk)

        if self.total_expected == self.total_worked:
            self.send_results(client_id)
            self.send_eof(client_id)
        return

    def recv_eof(self, total, client_id):
        self.total_expected = total
        if self.total_expected == self.total_worked:
            self.send_results(client_id)
            self.send_eof(client_id)
        return
