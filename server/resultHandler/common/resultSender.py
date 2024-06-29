import signal
import socket
import logging
from threading import Thread
from multiprocessing import Semaphore, Process

from common.poller import Poller

EOF_LINE = "EOF"


class ResultSender(Process):
    def __init__(self, ip, port, max_users, results_directory, directory_lock):
        super().__init__(name='ResultSender', args=())
        self.max_users = max_users
        self.results_directory = results_directory
        self.directory_lock = directory_lock

        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind((ip, port))
        self._server_socket.listen(1)
        self._server_on = True

        self._semaphore = Semaphore(value=self.max_users)
        self.pollers = []

    def run(self):
        signal.signal(signal.SIGTERM, self.__handle_signal)
        logging.info('action: run_sender_loop')
        while self._server_on:
            client_sock = self.__accept_new_connection()
            if not client_sock:
                continue
            ok = self.connect(client_sock)
            if not ok:
                client_sock.close()

        self._server_socket.close()
        logging.info('action: stop_sender_loop | result: success')

    def connect(self, client_sock):
        client_ip = client_sock.getpeername()[0]
        if self._semaphore.acquire(block=False):
            poller = Poller(client_sock, self.results_directory, self.directory_lock)
            thread = Thread(
                target=poller.run,
                args=(self._semaphore, )
            )
            self.pollers.append((poller, thread))
            thread.start()
            return True
        else:
            # TODO?: Maybe answer?
            logging.info(f'action: handle_client | ip: {client_ip} | result: fail | error: reached '
                         f'limit of simultaneous users {self.max_users} out of {self.max_users}')
            return False

    def __accept_new_connection(self):
        try:
            logging.debug('action: accept_connections | result: in_progress')
            c, addr = self._server_socket.accept()
            logging.debug(f'action: accept_connections | result: sucess | ip: {addr[0]}')
            return c
        except Exception as e:
            if self._server_on:
                logging.error(f'action: accept_connections | result: fail | error: {str(e)}')
            else:
                logging.debug('action: stop_accept_connections | result: success')
            return

    def __handle_signal(self, signum, frame):
        logging.debug('action: stop_sender | result: in_progress')
        self._server_on = False
        self._server_socket.close()

        for poller, _ in self.pollers:
            poller.stop()
        for _, thread in self.pollers:
            thread.join()
        logging.debug('action: stop_sender | result: success')
