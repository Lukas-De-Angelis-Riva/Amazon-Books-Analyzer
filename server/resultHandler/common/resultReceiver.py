import signal
import csv
import io
import logging
from multiprocessing import Process, Lock
from utils.serializer.q1OutSerializer import Q1OutSerializer
from utils.serializer.q2OutSerializer import Q2OutSerializer
from utils.serializer.q3OutSerializer import Q3OutSerializer
from utils.serializer.q5OutSerializer import Q5OutSerializer
from utils.protocol import is_eof

from common.resultHandlerMiddleware import ResultHandlerMiddleware

class ResultReceiver(Process):
    def __init__(self, file_name, file_lock):
        super().__init__(name='ResultReceiver', args=())
        self.serializers = {
            'Q1': Q1OutSerializer(),
            'Q2': Q2OutSerializer(),
            'Q3': Q3OutSerializer(),
            'Q4': Q3OutSerializer(),
            'Q5': Q5OutSerializer(),
        }
        self.eofs = {'Q1': False,'Q2': False, 'Q3': False, 'Q4': False, 'Q5': False}
        self.middleware = ResultHandlerMiddleware()
        self.file_lock = file_lock
        self.file_name = file_name
        self.stop_lock = Lock()

    def run(self):
        signal.signal(signal.SIGTERM, self.__handle_signal)

        logging.info(f'action: listen_results | result: in_progress')
        self.middleware.listen_results(self.save_results)
        logging.info(f'action: listen_results | result: success')

        logging.info(f'action: middleware_start | start')
        self.middleware.start()
        logging.info(f'action: middleware_start | end')

    def save_results(self, results_raw, results_type):
        results = self.deserialize_result(results_raw, results_type)

        with self.stop_lock, self.file_lock, open(self.file_name, 'a', encoding='UTF8') as file:
            writer = csv.writer(file)

            for result in results:
                logging.info(f'action: receive_result | result: success | value: {result}')
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

    def write_eof(self):
        with self.file_lock, open(self.file_name, 'a', encoding='UTF8') as file:
            writer = csv.writer(file)
            writer.writerow(['EOF'])
    
    def deserialize_result(self, bytes_raw, type):
        reader = io.BytesIO(bytes_raw)
        results = self.serializers[type].from_chunk(reader)        
        if is_eof(bytes_raw):
            self.eofs[type] = True
            logging.info(f'action: recv EOF {type}| result: success')
            if all(self.eofs.values()):
                self.write_eof()

        return results

    def __handle_signal(self, signum, frame):
        logging.info(f'action: stop_receiver | result: in_progress')
        self.middleware.stop()
        # leaving time to save_results to write and release file and file_lock
        self.stop_lock.acquire()
        self.stop_lock.release()
        logging.info(f'action: stop_receiver | result: success')
