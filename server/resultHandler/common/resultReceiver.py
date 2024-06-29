import logging
import signal
import io
import os

from multiprocessing import Process, Lock
from utils.serializer.q1OutSerializer import Q1OutSerializer    # type: ignore
from utils.serializer.q2OutSerializer import Q2OutSerializer    # type: ignore
from utils.serializer.q3OutSerializer import Q3OutSerializer    # type: ignore
from utils.serializer.q5OutSerializer import Q5OutSerializer    # type: ignore

from utils.persistentMap import PersistentMap
from utils.middleware.middleware import Middleware, ACK
from utils.model.message import Message, MessageType


IN_QUEUE = 'RH-Results'
QUERY1_ID = 'Q1'
QUERY2_ID = 'Q2'
QUERY3_ID = 'Q3'
QUERY4_ID = 'Q4'
QUERY5_ID = 'Q5'

BASE_DIRECTORY = "/clients"

TOTAL_BY_QUERY = "TOTAL_BY_QUERY"

TOTAL = "total"

EOF_LINE = "EOF"


class ClientTracker():
    def __init__(self, client_id, results_directory):
        if not os.path.exists(BASE_DIRECTORY):
            os.mkdir(BASE_DIRECTORY)

        if not os.path.exists(BASE_DIRECTORY + '/' + str(client_id)):
            os.mkdir(BASE_DIRECTORY + '/' + str(client_id))

        self.client_id = client_id
        self.queries = [QUERY1_ID, QUERY2_ID, QUERY3_ID, QUERY4_ID, QUERY5_ID]
        self.meta_data = PersistentMap(BASE_DIRECTORY + '/' + str(client_id) + '/' + "meta")

        self.meta_data[TOTAL_BY_QUERY] = {q: -1 for q in self.queries}

        self.results_path = results_directory + '/' + str(client_id) + '.csv'
        self.results_tmp_path = results_directory + '/tmp'
        if not os.path.exists(self.results_path):
            open(self.results_path, 'w').close()

        self.results_ptrs_path = self.results_path + '.ptrs'
        if not os.path.exists(self.results_ptrs_path):
            open(self.results_ptrs_path, 'wb').close()

        self.results = []

    def flush_results(self):
        with open(self.results_tmp_path, 'w') as tmp:
            tmp.writelines(self.results)
            tmp.flush()
        os.rename(self.results_tmp_path, self.results_path)

    def recovery(self):
        self.meta_data.load(lambda k, v: v)
        with open(self.results_path, 'r', encoding='utf-8') as fp:
            lines = fp.readlines()
        for line in lines:
            if line[-1] != '\n':
                self.flush_results()
            else:
                self.results.append(line)

        # Check if short write in ptrs file
        if os.path.exists(self.results_ptrs_path) and os.path.getsize(self.results_ptrs_path) % 4 != 0:
            s = 4 * (os.path.getsize(self.results_ptrs_path) // 4)
            os.truncate(self.results_ptrs_path, s)

    def is_completed(self):
        return all([x != -1 for x in self.meta_data[TOTAL_BY_QUERY].values()]) and \
            len(self.results) == sum(self.meta_data[TOTAL_BY_QUERY].values())


class ResultReceiver(Process):
    def __init__(self, results_directory, directory_lock, test_middleware=None):
        super().__init__(name='ResultReceiver', args=())
        self.serializers = {
            'Q1': Q1OutSerializer(),
            'Q2': Q2OutSerializer(),
            'Q3': Q3OutSerializer(),
            'Q4': Q3OutSerializer(),
            'Q5': Q5OutSerializer(),
        }

        self.middleware = test_middleware if test_middleware else Middleware()
        self.middleware.consume(IN_QUEUE, callback=self.save_results)

        self.directory_lock = directory_lock
        self.results_directory = results_directory
        self.stop_lock = Lock()

        self.clients = {}
        self.tracker = None

    def run(self):
        signal.signal(signal.SIGTERM, self.__handle_signal)

        logging.debug('action: middleware_start | start')
        self.middleware.start()
        logging.debug('action: middleware_start | end')

    def context_switch(self, client_id):
        if client_id not in self.clients:
            self.clients[client_id] = ClientTracker(client_id, self.results_directory)
            self.clients[client_id].recovery()
        self.tracker = self.clients[client_id]

    def result_to_string(self, result, result_type):
        if result_type == QUERY1_ID:
            return ', '.join([QUERY1_ID, str(result.title), str(result.authors), str(result.publisher)]) + '\n'
        elif result_type == QUERY2_ID:
            return ', '.join([QUERY2_ID, str(result)]) + '\n'
        elif result_type == QUERY3_ID:
            return ', '.join([QUERY3_ID, str(result.title), str(result.authors)]) + '\n'
        elif result_type == QUERY4_ID:
            return ', '.join([QUERY4_ID, str(result.title), str(result.authors)]) + '\n'
        elif result_type == QUERY5_ID:
            return ', '.join([QUERY5_ID, str(result)]) + '\n'
        return ""

    def write_ptr(self, fp):
        with open(self.tracker.results_ptrs_path, "ab+") as chunk_ptrs:
            curr = fp.tell()
            if chunk_ptrs.tell() >= 4:
                chunk_ptrs.seek(-4, io.SEEK_END)
                prev = chunk_ptrs.read(4)
                if prev != curr:
                    chunk_ptrs.write(int.to_bytes(fp.tell(), 4, "big"))
                    chunk_ptrs.flush()
            else:
                chunk_ptrs.write(int.to_bytes(fp.tell(), 4, "big"))
                chunk_ptrs.flush()

    def recv_results(self, results_raw, results_type):
        reader = io.BytesIO(results_raw)
        results = self.serializers[results_type].from_chunk(reader)

        complete_line = ""
        for r in results:
            line = self.result_to_string(r, results_type)
            if not line or line in self.tracker.results:
                continue
            else:
                self.tracker.results.append(line)
                complete_line += line

        if not complete_line:
            if self.tracker.is_completed() and (EOF_LINE+'\n') not in self.tracker.results:
                with self.stop_lock, self.directory_lock, open(self.tracker.results_path, 'a', encoding='UTF8') as file:
                    self.tracker.results.append(EOF_LINE + '\n')
                    file.write(EOF_LINE + '\n')
            return

        with self.stop_lock, self.directory_lock, open(self.tracker.results_path, 'a', encoding='UTF8') as file:
            self.write_ptr(fp=file)
            file.write(complete_line)

            if self.tracker.is_completed():
                self.tracker.results.append(EOF_LINE + '\n')
                file.write(EOF_LINE + '\n')
            file.flush()

    def recv_eof(self, results_type, expected):
        self.tracker.meta_data[TOTAL_BY_QUERY][results_type] = expected
        self.tracker.meta_data.flush()

        if self.tracker.is_completed():
            with self.stop_lock, self.directory_lock, open(self.tracker.results_path, 'a', encoding='UTF8') as file:
                self.write_ptr(fp=file)
                self.tracker.results.append(EOF_LINE + '\n')
                file.write(EOF_LINE + '\n')
                file.flush()

    def save_results(self, results_raw, results_type):
        msg = Message.from_bytes(results_raw)

        self.context_switch(msg.client_id)

        if msg.type == MessageType.EOF:
            self.recv_eof(results_type, msg.args[TOTAL])
        else:
            self.recv_results(msg.data, results_type)
        return ACK

    def __handle_signal(self, signum, frame):
        logging.debug('action: stop_receiver | result: in_progress')
        self.middleware.stop()
        # leaving time to save_results to write and release file and directory_lock
        self.stop_lock.acquire()
        self.stop_lock.release()
        logging.debug('action: stop_receiver | result: success')
