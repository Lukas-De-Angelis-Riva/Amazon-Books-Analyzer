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

    def process_chunk(self, chunk, client_id):
        raise RuntimeError("Must be redefined")

    def terminator(self, client_id):
        raise RuntimeError("Hasta la vista, baby.")

    def _all_chunks_received(self):
        return all(
            (self.total_by_worker[i] == self.worked_by_worker[i]) for i in range(1, self.n_workers+1)
        )

    def _all_eofs_received(self):
        return all(
            self.eof_workers.values()
        )

    def recv(self, raw_msg, key):
        msg = Message.from_bytes(raw_msg)
        if msg.type == MessageType.EOF:
            self._recv_eof(
                msg.args[WORKER_ID],
                msg.args[TOTAL],
                msg.client_id
            )
        elif msg.type == MessageType.DATA:
            self._recv_raw(
                msg.data,
                msg.args[WORKER_ID],
                msg.client_id
            )

        # TODO: return Middleware.ACK
        return True

    def _recv_raw(self, data, worker_id, client_id):
        reader = io.BytesIO(data)
        input_chunk = self.in_serializer.from_chunk(reader)
        self.process_chunk(input_chunk, client_id)
        self.worked_by_worker[worker_id] += len(input_chunk)

        if self._all_eofs_received() and self._all_chunks_received():
            self.terminator(client_id)
        return

    def _recv_eof(self, worker_id, total, client_id):
        logging.debug(f'action: recv_eof | client: {client_id} | worker_id: {worker_id} | status: success')
        self.eof_workers[worker_id] = True
        self.total_by_worker[worker_id] = total

        if self._all_eofs_received() and self._all_chunks_received():
            self.terminator(client_id)
        return
