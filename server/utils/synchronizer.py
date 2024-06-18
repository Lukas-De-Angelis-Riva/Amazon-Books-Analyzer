import logging
import io

from utils.middleware.middleware import ACK
from utils.listener import Listener
from utils.model.message import Message, MessageType

TOTAL = "total"
WORKER_ID = "worker_id"


class ClientTracker():
    def __init__(self, client_id, n_workers):
        self.client_id = client_id
        self.n_workers = n_workers
        self.eof_workers = {i: False for i in range(1, n_workers+1)}
        self.worked_by_worker = {i: 0 for i in range(1, n_workers+1)}
        self.total_by_worker = {i: -1 for i in range(1, n_workers+1)}

    def all_chunks_received(self):
        return all(
            (self.total_by_worker[i] == self.worked_by_worker[i]) for i in range(1, self.n_workers+1)
        )

    def all_eofs_received(self):
        return all(
            self.eof_workers.values()
        )

    def total_worked(self):
        return sum(self.total_by_worker.values())

    def __repr__(self) -> str:
        return f'ClientTracker({self.client_id})'

    def __str__(self) -> str:
        return self.__repr__()


class Synchronizer(Listener):
    def __init__(self, middleware, n_workers, in_serializer, out_serializer, chunk_size):
        super().__init__(middleware)
        self.chunk_size = chunk_size
        self.in_serializer = in_serializer
        self.out_serializer = out_serializer
        self.n_workers = n_workers
        self.clients = {}
        self.tracker = None

    def process_chunk(self, chunk):
        raise RuntimeError("Must be redefined")

    def terminator(self):
        raise RuntimeError("Hasta la vista, baby.")

    def recv(self, raw_msg, key):
        msg = Message.from_bytes(raw_msg)
        if msg.client_id not in self.clients:
            self.clients[msg.client_id] = ClientTracker(msg.client_id, self.n_workers)
        self.tracker = self.clients[msg.client_id]

        if msg.type == MessageType.EOF:
            self._recv_eof(
                msg.args[WORKER_ID],
                msg.args[TOTAL]
            )
        elif msg.type == MessageType.DATA:
            self._recv_raw(
                msg.data,
                msg.args[WORKER_ID]
            )

        return ACK

    def _recv_raw(self, data, worker_id):
        reader = io.BytesIO(data)
        input_chunk = self.in_serializer.from_chunk(reader)
        self.process_chunk(input_chunk)
        self.tracker.worked_by_worker[worker_id] += len(input_chunk)

        if self.tracker.all_eofs_received() and self.tracker.all_chunks_received():
            self.terminator()
        return

    def _recv_eof(self, worker_id, total):
        logging.debug(f'action: recv_eof | client: {self.tracker.client_id} | worker_id: {worker_id} | status: success')
        self.tracker.eof_workers[worker_id] = True
        self.tracker.total_by_worker[worker_id] = total

        if self.tracker.all_eofs_received() and self.tracker.all_chunks_received():
            self.terminator()
        return
