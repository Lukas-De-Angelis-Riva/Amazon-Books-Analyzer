import socket 
import signal
import logging

from common.clientHandlerMiddleware import ClientHandlerMiddleware
from utils.serializer.bookSerializer import BookSerializer
from utils.protocolHandler import ProtocolHandler
from utils.TCPhandler import SocketBroken
from utils.protocol import make_eof

class ClientHandler:
    def __init__(self, config_params):
        # Initialize server socket
        self.config_params = config_params
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', config_params['port']))
        self._server_socket.listen(1)
        self._server_on = True
        signal.signal(signal.SIGTERM, self.__handle_signal)

        self.book_serializer = BookSerializer()

        self.middleware = ClientHandlerMiddleware()
        
    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """
        while self._server_on:
            client_sock = self.__accept_new_connection() 
            if client_sock:
                self.__handle_client_connection(client_sock)

        self._server_socket.close()
        logging.debug(f'action: release_socket | result: success')
        self.middleware.stop()
        logging.debug(f'action: release_rabbitmq_conn | result: success')


    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            protocolHandler = ProtocolHandler(client_sock)
            keep_reading = True
            while keep_reading:
                t, value = protocolHandler.read()

                if protocolHandler.is_review(t):
                    keep_reading = self.__handle_reviews(value)
                elif protocolHandler.is_book(t):
                    keep_reading = self.__handle_books(value)
                elif protocolHandler.is_book_eof(t):
                    keep_reading = self.__handle_book_eof()
                #elif protocolHandler.is_flight_eof(t):
                #    keep_reading = self.__handle_flight_eof()

                protocolHandler.ack()
        
        except (SocketBroken,OSError) as e:
            logging.error(f'action: receive_message | result: fail | error: {e}')
        finally:
            if client_sock:
                logging.debug(f'action: release_client_socket | result: success')
                client_sock.close()
                logging.debug(f'action: finishing | result: success')

    def __handle_book_eof(self):
        eof = make_eof()
        self.middleware.send_booksQ1(eof)

        logging.debug(f'action: send_books | value: EOF | result: success')
        return False

    def __handle_books(self, value):
        data = self.book_serializer.to_bytes(value)
        self.middleware.send_booksQ1(data)

        logging.debug(f'action: send_books | len(value): {len(value)} | result: success')
        return True

    def __handle_review_eof(self):
        logging.debug(f'action: read review_eof | result: success')
        eof = make_eof(0)
        self.middleware.send_eof(eof)
        return False
        
    def __handle_reviews(self, reviews):
        #  It's responsible for separating the relevant 
        #  fields for each query and sending them to different queues.
        logging.debug(f'action: recived reviews | result: success | N: {len(reviews)}')

        # Q1:
        data = self.review_q1_serializer.to_bytes(reviews)
        self.middleware.send_reviewsQ1(data)

        # Q2:
        data = self.review_q2_serializer.to_bytes(reviews)
        self.middleware.send_reviewsQ2(data)

        # Q4:
        data = self.review_q4_serializer.to_bytes(reviews)
        self.middleware.send_reviewsQ4(data)

        return True

    def __accept_new_connection(self):
        """
        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """
        try:
            logging.debug('action: accept_connections | result: in_progress')
            c, addr = self._server_socket.accept()
            logging.debug(f'action: accept_connections | result: success | ip: {addr[0]}')
            return c
        except OSError as e:
            if self._server_on:
                logging.error(f'action: accept_connections | result: fail')
            else:
                logging.debug(f'action: stop_accept_connections | result: success')
            return

    def __handle_signal(self, signum, frame):
        """
        Close server socket graceful
        """
        logging.debug(f'action: stop_server | result: in_progress | singal {signum}')
        self._server_on = False
        self._server_socket.shutdown(socket.SHUT_RDWR)
        logging.debug(f'action: shutdown_socket | result: success')