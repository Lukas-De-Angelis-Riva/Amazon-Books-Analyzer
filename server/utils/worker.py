import logging
import shutil
import uuid
import io
import os

from utils.listener import Listener
from utils.clientTracker import ClientTracker
from utils.clientTracker import BASE_DIRECTORY, NULL_DIRECTORY
from utils.persistentList import PersistentList
from utils.model.message import Message, MessageType
from utils.middleware.middleware import ACK


TOTAL = "total"
WORKER_ID = "worker_id"

WORKED_CLIENTS_FILE_PATH = '/worked_clients'


class Worker(Listener):
    def __init__(self, middleware, in_serializer, out_serializer, peer_id, peers, chunk_size):
        super().__init__(middleware)
        self.peer_id = peer_id
        self.peers = peers
        self.chunk_size = chunk_size
        self.in_serializer = in_serializer
        self.out_serializer = out_serializer
        self.clients = {}
        self.worked_clients = PersistentList(WORKED_CLIENTS_FILE_PATH)
        self.tracker = None

    def forward_eof(self, eof):
        raise RuntimeError("Must be redefined")

    def forward_data(self, data):
        raise RuntimeError("Must be redefined")

    def work(self, input):
        return

    def do_after_work(self, chunk_id):
        return

    def terminator(self):
        raise RuntimeError("Must be redefined")

    def adapt_tracker(self):
        return

    def sign_uuid(self, u):
        b = int.to_bytes(self.peer_id, length=1, byteorder='big')
        b += u.bytes[1:]
        return uuid.UUID(bytes=b)

    def context_switch(self, client_id):
        if client_id not in self.clients:
            self.clients[client_id] = ClientTracker(client_id)
        self.tracker = self.clients[client_id]
        self.adapt_tracker()

    def recovery(self):
        self.worked_clients.load()
        if not os.path.exists(BASE_DIRECTORY):
            return
        for directory in os.listdir(BASE_DIRECTORY):
            if BASE_DIRECTORY + '/' + directory == NULL_DIRECTORY:
                shutil.rmtree(NULL_DIRECTORY)
                continue
            client_id = uuid.UUID(directory)
            if client_id in self.worked_clients:
                ClientTracker.clear(client_id)
                continue

            self.context_switch(client_id)
            self.tracker.recovery()

            if self.tracker.is_completed():
                self.terminator()
                self.worked_clients.append(client_id)
                ClientTracker.clear(client_id)
                del self.clients[self.tracker.client_id]
                self.tracker = None

    def send_chunk(self, chunk, chunk_id):
        logging.debug(f'action: send_results | status: in_progress | forwarding_chunk | len(chunk): {len(chunk)}')
        data = self.out_serializer.to_bytes(chunk)
        msg = Message(
            client_id=self.tracker.client_id,
            type=MessageType.DATA,
            data=data,
            args={
                WORKER_ID: self.peer_id,
            },
            ID=self.sign_uuid(chunk_id)
        )
        self.forward_data(msg.to_bytes())
        return

    def send_results(self, results):
        chunk = []
        id_iterator = iter(self.tracker.worked_chunks)
        logging.debug(f'action: send_results | status: in_progress | len(results): {len(results)}')
        for result in results:
            chunk.append(result)
            if len(chunk) >= self.chunk_size:
                self.send_chunk(chunk, next(id_iterator))
                chunk = []
        if chunk:
            self.send_chunk(chunk, next(id_iterator))
        logging.debug('action: send_results | status: success')
        return

    def send_eof(self, sent):
        eof = Message(
            client_id=self.tracker.client_id,
            type=MessageType.EOF,
            data=b'',
            args={
                TOTAL: sent,
                WORKER_ID: self.peer_id,
            },
            ID=self.tracker.eof_id()
        )
        self.forward_eof(eof.to_bytes())
        return

    def recv(self, raw_msg, key):
        msg = Message.from_bytes(raw_msg)

        if msg.client_id in self.worked_clients:
            return ACK

        self.context_switch(msg.client_id)

        if msg.ID in self.tracker.worked_chunks:
            return ACK

        if msg.type == MessageType.EOF:
            self.recv_eof(msg.args[TOTAL], msg.ID)
        else:
            self.recv_raw(msg.data, msg.ID)

        return ACK

    def recv_raw(self, data, chunk_id):
        reader = io.BytesIO(data)
        input_chunk = self.in_serializer.from_chunk(reader)
        logging.debug(f'action: recv_raw | status: new_chunk | len(chunk): {len(input_chunk)}')
        for input in input_chunk:
            self.work(input)
        sent = self.do_after_work(chunk_id)

        self.tracker.persist(chunk_id, flush_data=True, WORKED=len(input_chunk), SENT=sent)

        if self.tracker.is_completed():
            self.terminator()
            self.worked_clients.append(self.tracker.client_id)
            ClientTracker.clear(self.tracker.client_id)
            del self.clients[self.tracker.client_id]
            self.tracker = None
        return

    def recv_eof(self, total, eof_id):
        self.tracker.persist(eof_id, EXPECTED=total)

        if self.tracker.is_completed():
            self.terminator()
            self.worked_clients.append(self.tracker.client_id)
            ClientTracker.clear(self.tracker.client_id)
            del self.clients[self.tracker.client_id]
            self.tracker = None
        return
