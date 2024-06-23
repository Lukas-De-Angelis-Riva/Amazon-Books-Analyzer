import logging
import struct
import uuid
import os
from utils.protocolHandler import ProtocolHandler
from utils.serializer.lineSerializer import LineSerializer
from utils.protocol import make_eof, make_wait, TlvTypes

EOF_LINE = "EOF"


class Poller():
    def __init__(self, client_sock, results_directory, directory_lock):
        self.serializer = LineSerializer()
        #self.tcpHandler = TCPHandler(client_sock)
        self.protocolHandler = ProtocolHandler(client_sock)
        self.results_directory = results_directory
        self.directory_lock = directory_lock

        self.keep_reading = True

        self.chunk_size = 10
        self.cursor = 0
        self.eof_readed = False

    def wait_poll(self):
        return self.protocolHandler.read()

    def run(self, semaphore):
        try:
            self.client_id = self.protocolHandler.wait_handshake()
            self.file_name = self.results_directory + '/' + str(self.client_id) + '.csv'
            logging.debug(f'action: poller_run | client_id: {str(self.client_id)}')
            while self.keep_reading:
                self.wait_poll()
                chunk = self.poll()
                self.respond(chunk)

        except (SocketBroken, OSError) as e:
            if not self.eof_readed:
                logging.error(f'action: poller_run | result: fail | error: {str(e) or repr(e)}')

        self.protocolHandler.close()
        logging.debug('action: poller_stop | result: success')
        semaphore.release()

    def poll(self):
        logging.debug('action: poll | result: in_progress')
        if self.eof_readed or not os.path.exists(self.file_name):
            return []
        chunk = []
        with self.directory_lock, open(self.file_name, 'r', encoding='UTF8') as file:
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
        logging.debug('action: poll | result: success')
        return chunk

    def respond(self, chunk):
        if chunk:
            logging.debug(f'action: poller_respond | response: CHUNK[{len(chunk)}]')
            self.protocolHandler.send_lines(chunk)
        else:
            if self.eof_readed:
                logging.debug('action: poller_respond | response: EOF')
                self.protocolHandler.send_line_eof()
                self.stop()
            else:
                logging.debug('action: poller_respond | response: WAIT')
                # TODO: implement paging
                self.protocolHandler.send_wait()

    def stop(self):
        self.keep_reading = False
