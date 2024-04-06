import socket 
import signal
import logging
from common.handler import TCPHandler

class DummyClientHandler:
    def __init__(self, ip, port):
        # Initialize server socket
        self.ip = ip
        self.port = port
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(1)
        self._server_on = True

    def run(self):
        while self._server_on:
            client_sock = self.__accept_new_connection() 
            if client_sock:
                self.__handle_client_connection(client_sock)

        self._server_socket.close()

    def __handle_client_connection(self, client_sock):
        handler = TCPHandler(client_sock)
        handler.read(5)
        handler.send_all(str.encode("hello"))
        client_sock.close()

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
