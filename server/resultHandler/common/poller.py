import logging
import os
from utils.protocolHandler import ProtocolHandler
from utils.serializer.lineSerializer import LineSerializer
from utils.protocol import TlvTypes
from utils.TCPhandler import SocketBroken

EOF_LINE = "EOF"


class Poller():
    def __init__(self, client_sock, results_directory, directory_lock):
        self.serializer = LineSerializer()
        self.protocolHandler = ProtocolHandler(client_sock)
        self.results_directory = results_directory
        self.directory_lock = directory_lock

        self.keep_reading = True

        self.chunk_size = 10
        self.cursor = 0
        self.eof_readed = False

    def wait_poll(self):
        try:
            msg_type, msg_id, page = self.protocolHandler.read()

            if msg_type != TlvTypes.POLL:
                return 0, False
        except SocketBroken:
            return 0, False

        return page, True

    def run(self, semaphore):
        try:
            self.client_id = self.protocolHandler.wait_handshake()
            self.file_name = self.results_directory + '/' + str(self.client_id) + '.csv'
            logging.debug(f'action: poller_run | client_id: {str(self.client_id)}')
            while self.keep_reading:
                page, ok = self.wait_poll()
                if not ok:
                    continue
                chunk = self.poll(page)
                self.respond(chunk)

        except (SocketBroken, OSError) as e:
            if not self.eof_readed:
                logging.error(f'action: poller_run | result: fail | error: {str(e) or repr(e)}')

        self.protocolHandler.close()
        logging.debug('action: poller_stop | result: success')
        semaphore.release()

    def poll(self, page):
        logging.debug('action: poll | result: in_progress')
        if self.eof_readed or not os.path.exists(self.file_name):
            return []
        chunk = []

        with self.directory_lock, open(self.file_name, 'r', encoding='UTF8') as file:
            # page requested does not exist
            if page * 4 >= os.path.getsize(self.file_name + '.ptrs'):
                return []

            with open(self.file_name + '.ptrs', 'rb') as chunk_ptrs:
                chunk_ptrs.seek(page * 4)   # every ptr is an integer
                chunk_ptr = int.from_bytes(chunk_ptrs.read(4), "big")
                next_chunk_ptr = int.from_bytes(chunk_ptrs.read(4), "big")

            file.seek(chunk_ptr)

            while line := file.readline():
                logging.debug(f'action: read_line | line: {line}')
                if line.rstrip() == EOF_LINE:
                    self.eof_readed = True
                    return chunk
                chunk.append(line.rstrip())

                if file.tell() == next_chunk_ptr:
                    logging.debug('action: poll | result: success')
                    return chunk
            # self.cursor = file.tell()
        logging.debug('action: poll | result: success')
        return chunk

    def respond(self, chunk):
        if chunk:
            logging.debug(f'action: poller_respond | response: CHUNK[{len(chunk)}]')
            self.protocolHandler.send_lines(chunk)

        elif self.eof_readed:
            logging.debug('action: poller_respond | response: EOF')
            self.protocolHandler.send_line_eof()
            self.stop()
        else:
            logging.debug('action: poller_respond | response: WAIT')
            self.protocolHandler.send_wait()

    def stop(self):
        self.keep_reading = False
