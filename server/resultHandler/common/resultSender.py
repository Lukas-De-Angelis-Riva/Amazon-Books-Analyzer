import socket
import logging
import csv

from utils.TCPhandler import SocketBroken, TCPHandler
from utils.serializer.lineSerializer import LineSerializer
from utils.protocol import make_eof, make_wait, TlvTypes

EOF_LINE = "EOF"

class ResultSender():
    def __init__(self, ip, port, file_name, file_lock):
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind((ip, port))
        self._server_socket.listen(1)
        self._server_on = True

        self.file_name = file_name
        self.file_lock = file_lock
        self.chunk_size = 10
        self.serializer = LineSerializer()
        self.cursor = 0

        self.eof_readed = False

    def respond(self, tcpHandler, chunk):
        if chunk:
            data = self.serializer.to_bytes(chunk)
        else:
            if self.eof_readed:
                logging.debug('action: respond | response: EOF')
                data = make_eof()
            else:
                logging.debug('action: respond | response: WAIT')
                data = make_wait()
        tcpHandler.send_all(data)

    def poll(self):
        logging.debug('action: poll | result: in_progress')
        if self.eof_readed:
            return []

        chunk = []
        with self.file_lock, open(self.file_name, 'r', encoding='UTF8') as file:
            file.seek(self.cursor)
            while line := file.readline():
                logging.debug(f'action: read_line | line: {line}')
                if line.rstrip() == EOF_LINE:
                    self.eof_readed = True
                    return chunk

                chunk.append(line.rstrip())
                if len(chunk) >= self.chunk_size:
                    break
            self.cursor = file.tell()
        return chunk

    def run(self):
        while self._server_on:
            client_sock = self.__accept_new_connection() 
            if client_sock:
                self.__handle_client_connection(client_sock)

        self._server_socket.close()

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            logging.debug(f'action: handle_connection | conn: {client_sock}')
            tcpHandler = TCPHandler(client_sock)
            keep_reading = True
            while keep_reading:
                t = tcpHandler.read(TlvTypes.SIZE_CODE_MSG)
                chunk = self.poll()
                self.respond(tcpHandler, chunk)

        except (SocketBroken,OSError) as e:
            if not self.eof_readed:
                logging.error(f'action: receive_message | result: fail | error: {e}')
        finally:
            if client_sock:
                logging.debug(f'action: release_client_socket | result: success')
                client_sock.close()
                logging.debug(f'action: finishing | result: success')


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
                logging.info(f'action: stop_accept_connections | result: success')
            return