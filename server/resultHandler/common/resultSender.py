import signal
import socket
import logging
from multiprocessing import Process, Lock


from utils.TCPhandler import SocketBroken, TCPHandler
from utils.serializer.lineSerializer import LineSerializer
from utils.protocol import make_eof, make_wait, TlvTypes

EOF_LINE = "EOF"

class ResultSender(Process):
    def __init__(self, ip, port, file_name, file_lock):
        super().__init__(name='ResultSender', args=())
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

        self.client_lock = Lock()

    def respond(self, tcpHandler, chunk):
        if chunk:
            data = self.serializer.to_bytes(chunk)
        else:
            if self.eof_readed:
                logging.info('action: respond | response: EOF')
                data = make_eof()
            else:
                logging.info('action: respond | response: WAIT')
                data = make_wait()
        tcpHandler.send_all(data)

    def poll(self):
        logging.info('action: poll | result: in_progress')
        if self.eof_readed:
            return []

        chunk = []
        with self.file_lock, open(self.file_name, 'r', encoding='UTF8') as file:
            file.seek(self.cursor)
            while line := file.readline():
                logging.info(f'action: read_line | line: {line}')
                if line.rstrip() == EOF_LINE:
                    self.eof_readed = True
                    return chunk

                chunk.append(line.rstrip())
                if len(chunk) >= self.chunk_size:
                    break
            self.cursor = file.tell()
        return chunk

    def run(self):
        signal.signal(signal.SIGTERM, self.__handle_signal)

        while self._server_on:
            client_sock = self.__accept_new_connection() 
            if client_sock:
                self.__handle_client_connection(client_sock)

        self._server_socket.close()

    def __handle_client_connection(self, client_sock):
        try:
            self.client_lock.acquire()
            logging.info(f'action: handle_connection | conn: {client_sock}')
            tcpHandler = TCPHandler(client_sock)
            keep_reading = True
            while keep_reading:
                t = tcpHandler.read(TlvTypes.SIZE_CODE_MSG)
                chunk = self.poll()
                self.respond(tcpHandler, chunk)

        except (SocketBroken,OSError) as e:
            if not self.eof_readed:
                logging.info(f'action: receive_message | result: fail | error: {e}')
        finally:
            if client_sock:
                logging.info(f'action: release_client_socket | result: success')
                client_sock.close()
                logging.info(f'action: finishing | result: success')
            self.client_lock.release()

    def __accept_new_connection(self):
        try:
            logging.info('action: accept_connections | result: in_progress')
            c, addr = self._server_socket.accept()
            logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
            return c
        except Exception as e:
            if self._server_on:
                logging.error(f'action: accept_connections | result: fail | error: {str(e)}')
            else:
                logging.info(f'action: stop_accept_connections | result: success')
            return

    def __handle_signal(self, signum, frame):
        logging.info(f'action: stop_sender | result: in_progress')
        self._server_on = False
        self._server_socket.close()
        # leaving time to handle client, respond last poll and then terminate.
        self.client_lock.acquire() # next will be a semaphore waiting until 0.
        self.client_lock.release()
        logging.info(f'action: stop_sender | result: success')
