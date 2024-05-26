import logging
import signal

from utils.middleware.middleware import Middleware


class Listener2():
    def __init__(self, middleware: Middleware):
        signal.signal(signal.SIGTERM, self.__handle_signal)
        self.middleware = middleware
        self.exitcode = 0

    def run(self):
        self.middleware.start()
        return self.exitcode

    def __handle_signal(self, signum, frame):
        self.exitcode = -signum
        logging.debug('action: stop_handler | result: in_progress | signal {signum}')
        self.middleware.stop()
        logging.debug('action: stop_handler | result: success')
