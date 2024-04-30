import logging
import signal

from utils.middleware.middleware import Middleware
from utils.protocol import is_eof

class Listener():
    def __init__(self, middleware: Middleware):
        signal.signal(signal.SIGTERM, self.__handle_signal)
        self.middleware = middleware
        self.exitcode = 0

    def recv_raw(self, raw):
        raise RuntimeError("Must be redefined")

    def recv_eof(self, eof):
        raise RuntimeError("Must be redefined")

    def recv(self, raw):
        if is_eof(raw):
            self.recv_eof(raw)
            return False

        self.recv_raw(raw)
        return True

    def run(self):
        logging.debug(f'action: listen | result: in_progress')
        self.middleware.listen(self.recv)
        logging.debug(f'action: listen | result: success')

        self.middleware.start()
        return self.exitcode

    def __handle_signal(self, signum, frame):
        self.exitcode = -signum
        logging.debug(f'action: stop_handler | result: in_progress | signal {signum}')
        self.middleware.stop()
        logging.debug(f'action: stop_handler | result: success')