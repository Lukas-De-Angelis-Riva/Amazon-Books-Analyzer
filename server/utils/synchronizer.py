from utils.clientTrackerSynchronizer import ClientTrackerSynchronizer
from utils.model.message import Message, MessageType
from utils.middleware.middleware import ACK
from utils.listener import Listener

import logging
import io

TOTAL = "total"
WORKER_ID = "worker_id"

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
    
    def context_switch(self, client_id):
        if client_id not in self.clients:
            self.clients[client_id] = ClientTrackerSynchronizer(client_id, self.n_workers)
        self.tracker = self.clients[client_id]

    def recv(self, raw_msg, key):
        msg = Message.from_bytes(raw_msg)
        self.context_switch(msg.client_id)

        if msg.ID in self.tracker.worked_chunks:
            return ACK
        
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
        self.tracker.add_worked(len(input_chunk), worker_id)

        if self.tracker.all_chunks_received():
            self.terminator()
        return

    def _recv_eof(self, worker_id, total):
        logging.debug(f'action: recv_eof | client: {self.tracker.client_id} | worker_id: {worker_id} | status: success')
        self.tracker.set_total(total,worker_id)

        if self.tracker.all_chunks_received():
            self.terminator()
        return
