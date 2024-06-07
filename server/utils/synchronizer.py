import logging
import io

from utils.listener import Listener
from utils.model.message import Message, MessageType

TOTAL = "total"
WORKER_ID = "worker_id"


class Synchronizer(Listener):
    def __init__(self, middleware, n_workers, in_serializer, out_serializer, chunk_size):
        super().__init__(middleware)
        self.chunk_size = chunk_size
        self.in_serializer = in_serializer
        self.out_serializer = out_serializer

        self.n_workers = n_workers
        self.eof_workers = {i: False for i in range(1, n_workers+1)}
        self.worked_by_worker = {i: 0 for i in range(1, n_workers+1)}
        self.total_by_worker = {i: -1 for i in range(1, n_workers+1)}

    def forward_eof(self, eof):
        raise RuntimeError("Must be redefined")

    def forward_data(self, data):
        raise RuntimeError("Must be redefined")

    def process_chunk(self, chunk):
        raise RuntimeError("Must be redefined")

    def send_eof(self, client_id):
        eof = Message(
            client_id=client_id,
            type=MessageType.EOF,
            data=b'',
            args={
                # TODO: REFACTOR!!
                # QUERY1, QUERY2, QUERY3 this IDEA
                TOTAL: sum(self.total_by_worker.values())

                # QUERY4, QUERY5 PRE-PROCESS, THEN SEND. SOMETHING LIKE:
                # TOTAL = len(self.results) // Query4: máx 10, Query5: ~10%
            }
        )
        self.forward_eof(eof)
        return

    def all_chunks_received(self):
        return all(
            (self.total_by_worker[i] == self.worked_by_worker[i]) for i in range(1, self.n_workers+1)
        )

    def all_eofs_received(self):
        return all(
            self.eof_workers.values()
        )

    def recv(self, raw_msg, key):
        msg = Message.from_bytes(raw_msg)
        if msg.type == MessageType.EOF:
            self.recv_eof(
                msg.args[WORKER_ID],
                msg.args[TOTAL],
                msg.client_id
            )
        else:
            self.recv_raw(
                msg.data,
                msg.args[WORKER_ID],
                msg.client_id
            )

        # TODO: return Middleware.ACK
        return True

    def recv_raw(self, data, worker_id, client_id, key):
        reader = io.BytesIO(data)
        input_chunk = self.in_serializer.from_chunk(reader)
        self.process_chunk(input_chunk)
        self.worked_by_worker[worker_id] += len(input_chunk)

        if self.all_eofs_received() and self.all_chunks_received():
            self.send_eof(client_id)
        return

    def recv_eof(self, worker_id, total, client_id):
        logging.debug(f'action: recv_eof | client: {client_id} | worker_id: {worker_id} | status: success')
        self.eof_workers[worker_id] = True
        self.total_by_worker[worker_id] = total

        if self.all_eofs_received() and self.all_chunks_received():
            self.send_eof(client_id)
        return
