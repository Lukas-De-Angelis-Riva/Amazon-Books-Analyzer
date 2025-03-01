import logging
import signal
from multiprocessing import Lock
from common.resultReceiver import ResultReceiver
from common.resultSender import ResultSender


class ResultHandler():
    def __init__(self, config_params):
        signal.signal(signal.SIGTERM, self.__handle_signal)
        self.lock = Lock()
        self.results_directory = config_params['results_directory']
        self.ip = config_params['ip']
        self.port = config_params['port']
        self.max_users = config_params['max_users']

    def run(self):
        self.psnd = ResultReceiver(self.results_directory, self.lock)
        self.prcv = ResultSender(self.ip, self.port, self.max_users, self.results_directory, self.lock)

        self.psnd.start()
        self.prcv.start()
        logging.info('action: run handler | result: success')
        self.psnd.join()
        self.prcv.join()
        logging.info('action: stop_handler | result: sucess')

    def __handle_signal(self, signum, frame):
        logging.info(f'action: stop_handler | result: in_progress | signal: SIGTERM({signum})')
        self.psnd.terminate()
        self.prcv.terminate()
