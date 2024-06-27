import socket
import signal
import logging
import time
from threading import Thread, Event
from multiprocessing import Semaphore
from utils.protocolHandler import ProtocolHandler
from utils.TCPhandler import SocketBroken
from common.queryManager import QueryManager, QUERY1_ID, QUERY2_ID, QUERY3_ID, QUERY5_ID


class ClientHandler:
    def __init__(self, config_params):
        signal.signal(signal.SIGTERM, self.__handle_signal)

        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', config_params['port']))
        self._server_socket.listen(1)
        self._server_on = True
        self.max_users = config_params['max_users']
        self._semaphore = Semaphore(value=self.max_users)
        self._threads = []
        self._thread_stoppers = []
        self.workers_by_query = {
            QUERY1_ID: config_params['n_workers_q1'],
            QUERY2_ID: config_params['n_workers_q2'],
            QUERY3_ID: config_params['n_workers_q3'],
            QUERY5_ID: config_params['n_workers_q5']
        }

    def run(self):
        logging.info('action: run_server')
        while self._server_on:
            client_sock = self.__accept_new_connection()
            if not client_sock:
                continue
            ok = self.connect(client_sock)
            if not ok:
                client_sock.close()

        self._server_socket.close()
        logging.debug('action: release_socket | result: success')
        logging.info('action: stop_server | result: success')

    def connect(self, client_sock):
        client_ip = client_sock.getpeername()[0]
        if self._semaphore.acquire(block=False):
            thread_stopper = Event()
            thread = Thread(
                target=self.__handle_client_connection,
                args=(client_sock, thread_stopper, )
            )
            self._threads.append(thread)
            self._thread_stoppers.append(thread_stopper)
            thread.start()
            return True
        else:
            # TODO?: Maybe answer?
            logging.info(f'action: handle_client | ip: {client_ip} | result: fail | error: reached '
                         f'limit of simultaneous users {self.max_users} out of {self.max_users}')
            return False

    def __handle_client_connection(self, client_sock, event_stop):
        addr = client_sock.getpeername()[0]
        protocolHandler = ProtocolHandler(client_sock)
        try:
            client_id = protocolHandler.wait_handshake()
            logging.info(f'action: handle_client | ip: {addr} | uuid: {str(client_id)}')
            manager = QueryManager(client_id, workers_by_query=self.workers_by_query)
            keep_reading = True
            while keep_reading and not event_stop.is_set():
                t, msg_id, value = protocolHandler.read()

                if protocolHandler.is_review(t):
                    manager.distribute_reviews(msg_id, value)
                    logging.debug(f'action: send_reviews | N: {len(value)} | result: success')
                    time.sleep(0.5)
                elif protocolHandler.is_book(t):
                    manager.distribute_books(msg_id, value)
                    logging.debug(f'action: send_books | N: {len(value)} | result: success')
                elif protocolHandler.is_book_eof(t):
                    manager.terminate_books()
                    logging.debug('action: send_books | value: EOF | result: success')
                elif protocolHandler.is_review_eof(t):
                    manager.terminate_reviews()
                    keep_reading = False
                    logging.debug('action: review_eof | value: EOF | result: success')
                protocolHandler.ack()
            manager.stop()
        except (SocketBroken, OSError) as e:
            logging.error(f'action: receive_message | result: fail | error: {str(e) or repr(e)}')

        logging.debug('action: release_client_socket | result: success')
        client_sock.close()
        logging.debug('action: finishing | result: success')
        self._semaphore.release()
        logging.info(f'action: handle_client | result: end | ip: {addr} | uuid: {str(client_id)}')

    def __accept_new_connection(self):
        try:
            logging.debug('action: accept_connections | result: in_progress')
            c, addr = self._server_socket.accept()
            logging.debug(f'action: accept_connections | result: success | ip: {addr[0]}')
            logging.info(f'action: new client | ip: {addr[0]}')
            return c
        except OSError as e:
            if self._server_on:
                logging.error(f'action: accept_connections | result: fail | error: {str(e) or repr(e)}')
            else:
                logging.debug('action: stop_accept_connections | result: success')
            return

    def __handle_signal(self, signum, frame):
        logging.info(f'action: stop_server | result: in_progress | signal {signum}')
        self._server_on = False
        self._server_socket.shutdown(socket.SHUT_RDWR)
        for stopper in self._thread_stoppers:
            stopper.set()
        for thread in self._threads:
            thread.join()
        logging.debug('action: shutdown_socket | result: success')
