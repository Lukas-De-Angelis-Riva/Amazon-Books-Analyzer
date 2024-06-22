import logging
import shutil
import uuid
import io
import os

from utils.clientTrackerSynchronizer import ClientTrackerSynchronizer
from utils.clientTrackerSynchronizer import BASE_DIRECTORY, NULL_DIRECTORY
from utils.model.message import Message, MessageType
from utils.middleware.middleware import ACK
from utils.listener import Listener

# TEST PURPOSES
from utils.model.virus import virus

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

    def process_chunk(self, chunk, chunk_id):
        raise RuntimeError("Must be redefined")

    def terminator(self):
        raise RuntimeError("Hasta la vista, baby.")

    def adapt_tracker(self):
        return

    def context_switch(self, client_id):
        if client_id not in self.clients:
            self.clients[client_id] = ClientTrackerSynchronizer(client_id, self.n_workers)
        self.tracker = self.clients[client_id]
        self.adapt_tracker()

    def recovery(self):
        if not os.path.exists(BASE_DIRECTORY):
            return
        for directory in os.listdir(BASE_DIRECTORY):
            if BASE_DIRECTORY + '/' + directory == NULL_DIRECTORY:
                shutil.rmtree(NULL_DIRECTORY)
                continue
            client_id = uuid.UUID(directory)
            self.context_switch(client_id)
            virus.infect()
            self.tracker.recovery()
            virus.infect()

            if self.tracker.all_chunks_received():
                virus.infect()
                self.terminator()
                virus.infect()
                self.tracker.clear()
                virus.infect()
                del self.clients[self.tracker.client_id]
                self.tracker = None

    def recv(self, raw_msg, key):
        msg = Message.from_bytes(raw_msg)
        self.context_switch(msg.client_id)

        if msg.ID in self.tracker.worked_chunks:
            return ACK

        if msg.type == MessageType.EOF:
            self._recv_eof(
                str(msg.args[WORKER_ID]),
                msg.args[TOTAL],
                msg.ID
            )
        elif msg.type == MessageType.DATA:
            self._recv_raw(
                msg.data,
                msg.ID,
                str(msg.args[WORKER_ID])
            )

        return ACK

    def _recv_raw(self, data, chunk_id, worker_id):
        reader = io.BytesIO(data)
        input_chunk = self.in_serializer.from_chunk(reader)
        self.process_chunk(input_chunk, chunk_id)

        virus.infect()
        self.tracker.persist(chunk_id, worker_id, worked=len(input_chunk))
        virus.infect()

        if self.tracker.all_chunks_received():
            virus.infect()
            self.terminator()
            virus.infect()
            self.tracker.clear()
            virus.infect()
            del self.clients[self.tracker.client_id]
            self.tracker = None
        return

    def _recv_eof(self, worker_id, total, eof_id):
        logging.debug(f'action: recv_eof | client: {self.tracker.client_id} | worker_id: {worker_id} | status: success')
        self.tracker.persist(eof_id, worker_id, total=total)

        if self.tracker.all_chunks_received():
            virus.infect()
            self.terminator()
            virus.infect()
            self.tracker.clear()
            virus.infect()
            del self.clients[self.tracker.client_id]
            self.tracker = None
        return
