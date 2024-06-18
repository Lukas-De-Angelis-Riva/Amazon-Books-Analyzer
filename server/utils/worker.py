import json
import logging
import io
import os
import csv

from utils.listener import Listener
from utils.model.message import Message, MessageType
from utils.middleware.middleware import ACK

TOTAL = "total"
WORKER_ID = "worker_id"


BEGIN = "BEGIN"
WRITE = "WRITE"
COMMIT = "COMMIT"
EXPECTED = "EXPECT"
WORKED = "WORKED"
SENT = "SENT"


class LogManager():
    def __init__(self, client_id):
        self.client_id = client_id
        os.mkdir(str(client_id))
        # OJO PORQUE BORRA TODO AL CREARSE, VERIFICAR SI EXISTE Y SI EXISTE NO CREARLO.
        open(str(client_id) + '/log', 'w').close()
        open(str(client_id) + '/data', 'w').close()
        open(str(client_id) + '/tracker', 'w').close()
        open(str(client_id) + '/chunks', 'w').close()

        self.changes = {}

    def begin(self, chunk_id):
        file_name = str(self.client_id) + '/log'
        with open(file_name, "w", encoding='UTF-8') as log_file:
            log = csv.writer(log_file, delimiter=";")
            log.writerow([BEGIN, str(chunk_id)])

    def hold_change(self, k, v_old, v_new):
        if v_old == v_new:
            return
        if k not in self.changes:
            self.changes[k] = [v_old, v_new]
        else:
            self.changes[k][1] = v_new

    def flush_changes(self):
        file_name = str(self.client_id) + '/log'
        with open(file_name, "a+") as log_file:
            log = csv.writer(log_file, delimiter=";")
            for k in self.changes:
                old = self.changes[k][0]
                log.writerow([WRITE, k, old])

        self.changes = {}

    def log_change(self, key, v_old):
        file_name = str(self.client_id) + '/log'
        with open(file_name, "a+") as log_file:
            log = csv.writer(log_file, delimiter=";")
            log.writerow([WRITE, key, v_old])

    def flush_data(self, data_json):
        file_name = str(self.client_id) + '/data'
        with open(file_name, "w") as data_file:
            json.dump(data_json, data_file, indent=4)

    def flush_tracker(self, data_json):
        file_name = str(self.client_id) + '/tracker'
        with open(file_name, "w") as data_file:
            json.dump(data_json, data_file, indent=4)

    def commit(self, chunk_id):
        file_name = str(self.client_id) + '/log'
        with open(file_name, "a+") as log_file:
            log = csv.writer(log_file, delimiter=";")
            log.writerow([COMMIT, str(chunk_id)])

        file_name = str(self.client_id) + '/chunks'
        with open(file_name, "a+") as bucket_file:
            bucket = csv.writer(bucket_file, delimiter=";")
            bucket.writerow([str(chunk_id)])


class ClientTracker():
    def __init__(self, client_id):
        self.client_id = client_id
        self.log_manager = LogManager(client_id)
        self.results = {}
        self.worked_chunks = []
        self.total_expected = -1
        self.total_worked = 0
        self.total_sent = 0

    def expect(self, expected):
        self.total_expected = expected

    def is_completed(self):
        return self.total_expected == self.total_worked

    def flush_data(self):
        self.log_manager.flush_data({
            k: v.encode()
            for k, v in self.results.items()
        })
        self.log_manager.flush_tracker(
            {
                EXPECTED: self.total_expected,
                WORKED: self.total_worked,
                SENT: self.total_sent,
            }
        )

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

        self.recovery()

    def recovery(self):
        for directory in os.listdir('/clients'):
            client_id = directory
            self.context_switch(client_id)
            self.tracker.results = json.load(f'/clients/{client_id}/data')
            self.tracker.total = json.load(f'/clients/{client_id}/tracker')
            self.tracker.worked_chunks = [csv.read(f'/clients/{client_id}/chunks')]
            self.tracker.undo(f'/clients/{client_id}/log')
            self.tracker.flush_all()

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

    def context_switch(self, client_id):
        if client_id not in self.clients:
            self.clients[client_id] = ClientTracker(client_id)
        self.tracker = self.clients[client_id]

    def recv(self, raw_msg, key):
        msg = Message.from_bytes(raw_msg)
        self.context_switch(msg.client_id)
        if msg.ID in self.tracker.worked_chunks:
            return ACK
        elif msg.type == MessageType.EOF:
            self.recv_eof(msg.args[TOTAL])
        else:
            self.recv_raw(msg.data, msg.ID)

        return ACK

    def recv_raw(self, data, chunk_id):
        self.tracker.log_manager.begin(chunk_id)

        reader = io.BytesIO(data)
        input_chunk = self.in_serializer.from_chunk(reader)
        logging.debug(f'action: recv_raw | status: new_chunk | len(chunk): {len(input_chunk)}')
        for input in input_chunk:
            self.work(input)
        self.do_after_work()
        self.tracker.log_manager.log_change(WORKED, self.tracker.total_worked)
        self.tracker.log_manager.flush_changes()
        self.tracker.total_worked += len(input_chunk)
        self.tracker.worked_chunks.append(chunk_id)
        self.tracker.flush_data()
        self.tracker.log_manager.commit(chunk_id)

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
