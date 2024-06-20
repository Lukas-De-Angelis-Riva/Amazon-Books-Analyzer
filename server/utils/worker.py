import json
import logging
import io
import os
import csv
import uuid

from utils.listener import Listener
from utils.model.message import Message, MessageType
from utils.model.log import LogFactory, LogLineType
from utils.model.log import WriteLine, WriteMetadataLine, BeginLine, CommitLine
from utils.middleware.middleware import ACK

TOTAL = "total"
WORKER_ID = "worker_id"


EXPECTED = "EXPECT"
WORKED = "WORKED"
SENT = "SENT"


class LogManager():
    BASE_DIRECTORY = '/clients'

    def __init__(self, client_id):
        self.client_id = client_id

        if not os.path.exists(LogManager.BASE_DIRECTORY):
            os.mkdir(LogManager.BASE_DIRECTORY)

        if not os.path.exists(LogManager.BASE_DIRECTORY + '/' + str(client_id)):
            os.mkdir(LogManager.BASE_DIRECTORY + '/' + str(client_id))

        self.log_file = LogManager.BASE_DIRECTORY + '/' + str(client_id) + '/log'
        if not os.path.exists(self.log_file):
            open(self.log_file, 'w').close()

        self.data_file = LogManager.BASE_DIRECTORY + '/' + str(client_id) + '/data'
        if not os.path.exists(self.data_file):
            open(self.data_file, 'w').close()

        self.tracker_file = LogManager.BASE_DIRECTORY + '/' + str(client_id) + '/tracker'
        if not os.path.exists(self.tracker_file):
            open(self.tracker_file, 'w').close()

        self.chunks_file = LogManager.BASE_DIRECTORY + '/' + str(client_id) + '/chunks'
        if not os.path.exists(self.chunks_file):
            open(self.chunks_file, 'w').close()

        self.tmp_file = LogManager.BASE_DIRECTORY + '/' + str(client_id) + '/tmp'

        self.changes = {}

    def begin(self, chunk_id):
        with open(self.log_file, "w", encoding='UTF-8') as log_file:
            begin_line = BeginLine(chunk_id)
            log_file.write(begin_line.to_line())

    def hold_change(self, k, v_old, v_new):
        if k not in self.changes:
            self.changes[k] = [v_old, v_new]
        else:
            self.changes[k][1] = v_new

    def log_changes(self):
        with open(self.log_file, "a+") as log_file:
            for k in self.changes:
                old = self.changes[k][0].encode()
                write_line = WriteLine(k, old)
                log_file.write(write_line.to_line())

        self.changes = {}

    def log_metadata(self, key, v_old):
        with open(self.log_file, "a+") as log_file:
            write_line = WriteMetadataLine(key, v_old)
            log_file.write(write_line.to_line())

    def flush_data(self, data_json):
        with open(self.tmp_file, "w") as tmp_fp:
            json.dump(data_json, tmp_fp, indent=4)

        # It's assumed that the rename operation is atomic.
        os.rename(self.tmp_file, self.data_file)

    def flush_tracker(self, data_json):
        with open(self.tmp_file, "w") as fp:
            json.dump(data_json, fp, indent=4)

        # It's assumed that the rename operation is atomic.
        os.rename(self.tmp_file, self.tracker_file)

    def commit(self, chunk_id):
        with open(self.log_file, "a+") as log_file:
            commit_line = CommitLine(chunk_id)
            log_file.write(commit_line.to_line())

        with open(self.chunks_file, "a+") as bucket_file:
            bucket = csv.writer(bucket_file, delimiter=";")
            bucket.writerow([str(chunk_id)])

    def append_chunk_id(self, chunk_id):
        with open(self.chunks_file, "a+") as bucket_file:
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
        # DUMMY PARSER
        self.parser = lambda k, v: v

    def undo(self):
        with open(self.log_manager.log_file, 'r') as f:
            aux = f.readlines()
        log_lines = LogFactory.from_lines(aux)
        if not log_lines:
            return
        if log_lines[-1].type == LogLineType.COMMIT:
            chunk_id = log_lines[-1].chunk_id
            if chunk_id not in self.worked_chunks:
                self.worked_chunks.append(chunk_id)
                self.log_manager.append_chunk_id(chunk_id)
            return

        log_lines.reverse()
        for log_line in log_lines:

            if log_line.type == LogLineType.WRITE:
                self.results[log_line.key] = self.parser(log_line.key, log_line.old_value)

            elif log_line.type == LogLineType.WRITE_METADATA:
                if log_line.key == EXPECTED:
                    self.total_expected = log_line.old_value
                elif log_line.key == WORKED:
                    self.total_worked = log_line.old_value
                elif log_line.key == SENT:
                    self.total_sent = log_line.old_value

            elif log_line.type == LogLineType.BEGIN:
                break
        self.flush_data()

    def recovery(self):
        if os.path.getsize(self.log_manager.tracker_file) > 0:
            with open(self.log_manager.tracker_file, 'r') as f:
                aux = json.load(f)
            self.total_expected = aux[EXPECTED]
            self.total_worked = aux[WORKED]
            self.total_sent = aux[SENT]

        if os.path.getsize(self.log_manager.chunks_file) > 0:
            with open(self.log_manager.chunks_file, 'r') as f:
                aux = f.readlines()
            self.worked_chunks = [uuid.UUID(u.strip()) for u in aux]

        if os.path.getsize(self.log_manager.data_file) > 0:
            with open(self.log_manager.data_file, 'r') as f:
                aux = json.load(f)
            self.results = {
                k: self.parser(k, v)
                for k, v in aux.items()
            }

        if os.path.getsize(self.log_manager.log_file) > 0:
            self.undo()

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

    def forward_eof(self, eof):
        raise RuntimeError("Must be redefined")

    def forward_data(self, data):
        raise RuntimeError("Must be redefined")

    def work(self, input):
        return

    def do_after_work(self):
        return

    def adapt_tracker(self):
        return

    def context_switch(self, client_id):
        if client_id not in self.clients:
            self.clients[client_id] = ClientTracker(client_id)
        self.tracker = self.clients[client_id]
        self.adapt_tracker()

    def recovery(self):
        if not os.path.exists(LogManager.BASE_DIRECTORY):
            return
        for directory in os.listdir(LogManager.BASE_DIRECTORY):
            client_id = uuid.UUID(directory)
            self.context_switch(client_id)
            self.tracker.recovery()

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
        self.tracker.log_manager.log_metadata(WORKED, self.tracker.total_worked)
        self.tracker.log_manager.log_changes()
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
