import signal
import csv
import io
import logging
from multiprocessing import Process, Lock
from utils.serializer.q1OutSerializer import Q1OutSerializer    # type: ignore
from utils.serializer.q2OutSerializer import Q2OutSerializer    # type: ignore
from utils.serializer.q3OutSerializer import Q3OutSerializer    # type: ignore
from utils.serializer.q5OutSerializer import Q5OutSerializer    # type: ignore

from utils.middleware.middleware import Middleware, ACK
from utils.model.message import Message, MessageType

IN_QUEUE = 'RH-Results'
QUERY1_ID = 'Q1'
QUERY2_ID = 'Q2'
QUERY3_ID = 'Q3'
QUERY4_ID = 'Q4'
QUERY5_ID = 'Q5'

TOTAL = "total"


class ClientTracker():
    def __init__(self, client_id):
        self.client_id = client_id
        self.queries = [QUERY1_ID, QUERY2_ID, QUERY3_ID, QUERY4_ID, QUERY5_ID]
        self.eofs = {
            q: False for q in self.queries
        }
        self.total_results_received = {
            q: 0 for q in self.queries
        }

        self.total_results_expected = {
            q: -1 for q in self.queries
        }

    def eof(self, query_id):
        assert query_id in self.queries
        self.eofs[query_id] = True

    def expect(self, query_id, total):
        assert query_id in self.queries
        self.total_results_expected[query_id] = total

    def add(self, query_id, amount):
        assert query_id in self.queries
        self.total_results_received[query_id] += amount

    def all_eofs_received(self):
        return all(
            self.eofs.values()
        )

    def all_chunks_received(self):
        return all(
            self.total_results_received[q] == self.total_results_expected[q]
            for q in self.queries
        )


class ResultReceiver(Process):
    def __init__(self, results_directory, directory_lock):
        super().__init__(name='ResultReceiver', args=())
        self.serializers = {
            'Q1': Q1OutSerializer(),
            'Q2': Q2OutSerializer(),
            'Q3': Q3OutSerializer(),
            'Q4': Q3OutSerializer(),
            'Q5': Q5OutSerializer(),
        }
        self.middleware = Middleware()
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
            self.clients[client_id] = ClientTracker(client_id)
        self.tracker = self.clients[client_id]

    def save_results(self, results_raw, results_type):
        msg = Message.from_bytes(results_raw)
        self.context_switch(msg.client_id)

        results = self.deserialize_result(msg, results_type)
        logging.debug(f'action: save_results({results_type}) | result: in_progress | n: {len(results)}')
        if len(results) == 0:
            logging.debug(f'action: save_results({results_type}) | result: success')
            return ACK

        file_name = self.results_directory + '/' + str(self.tracker.client_id) + '.csv'

        with self.stop_lock, self.directory_lock, open(file_name, 'a', encoding='UTF8') as file:
            writer = csv.writer(file)

            for result in results:
                logging.debug(f'action: save_result({results_type})_i | result: success | value: {result}')
                if results_type == 'Q1':
                    writer.writerow(['Q1', result.title, result.authors, result.publisher])
                elif results_type == 'Q2':
                    writer.writerow(['Q2', result])
                elif results_type == 'Q3':
                    writer.writerow(['Q3', result.title, result.authors])
                elif results_type == 'Q4':
                    writer.writerow(['Q4', result.title, result.authors])
                elif results_type == 'Q5':
                    writer.writerow(['Q5', result])
                else:
                    continue

            self.tracker.add(results_type, len(results))

            if self.tracker.all_eofs_received() and self.tracker.all_chunks_received():
                self.write_eof()

        logging.debug(f'action: save_results({results_type}) | result: success')
        return ACK

    def write_eof(self):
        file_name = self.results_directory + '/' + str(self.tracker.client_id) + '.csv'
        with self.stop_lock, self.directory_lock, open(file_name, 'a', encoding='UTF8') as file:
            writer = csv.writer(file)
            writer.writerow(['EOF'])

    def deserialize_result(self, msg, type):
        if msg.type == MessageType.EOF:
            self.tracker.eof(type)
            self.tracker.expect(type, msg.args[TOTAL])
            logging.debug(f'action: recv EOF {type} | client: {self.tracker.client_id}')
            if self.tracker.all_eofs_received() and self.tracker.all_chunks_received():
                self.write_eof()
                logging.debug(f'action: write EOF | client: {self.tracker.client_id}')
            else:
                logging.debug(f'action: write EOF | client: {self.tracker.client_id} | postponed')
            return []

        reader = io.BytesIO(msg.data)
        results = self.serializers[type].from_chunk(reader)
        return results

    def __handle_signal(self, signum, frame):
        logging.debug('action: stop_receiver | result: in_progress')
        self.middleware.stop()
        # leaving time to save_results to write and release file and directory_lock
        self.stop_lock.acquire()
        self.stop_lock.release()
        logging.debug('action: stop_receiver | result: success')
