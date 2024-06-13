import uuid
import socket
import signal
import logging

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
        self.workers_by_query = {
            QUERY1_ID: config_params['n_workers_q1'],
            QUERY2_ID: config_params['n_workers_q2'],
            QUERY3_ID: config_params['n_workers_q3'],
            QUERY5_ID: config_params['n_workers_q5']
        }

    def run(self):
        logging.info('action: run server | result: success')
        while self._server_on:
            client_sock = self.__accept_new_connection()
            if client_sock:
                self.__handle_client_connection(client_sock)

        self._server_socket.close()
        logging.debug('action: release_socket | result: success')
        logging.info('action: stop_server | result: success')

    def __handle_client_connection(self, client_sock):
        client_id = uuid.uuid4()
        try:
            manager = QueryManager(client_id, workers_by_query=self.workers_by_query)

            protocolHandler = ProtocolHandler(client_sock)
            keep_reading = True
            while keep_reading:
                t, value = protocolHandler.read()

                if protocolHandler.is_review(t):
                    manager.distribute_reviews(value)
                    logging.debug(f'action: send_reviews | N: {len(value)} | result: success')
                elif protocolHandler.is_book(t):
                    manager.distribute_books(value)
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
            logging.error(f'action: receive_message | result: fail | error: {str(e)}')
        finally:
            if client_sock:
                logging.debug('action: release_client_socket | result: success')
                client_sock.close()
                logging.debug('action: finishing | result: success')

    def __accept_new_connection(self):
        try:
            logging.debug('action: accept_connections | result: in_progress')
            c, addr = self._server_socket.accept()
            logging.debug(f'action: accept_connections | result: success | ip: {addr[0]}')
            logging.info(f'action: new client | ip: {addr[0]}')
            return c
        except OSError as e:
            if self._server_on:
                logging.error(f'action: accept_connections | result: fail | error: {str(e)}')
            else:
                logging.debug('action: stop_accept_connections | result: success')
            return

    def __handle_signal(self, signum, frame):
        logging.info(f'action: stop_server | result: in_progress | signal {signum}')
        self._server_on = False
        self._server_socket.shutdown(socket.SHUT_RDWR)
        logging.debug('action: shutdown_socket | result: success')
