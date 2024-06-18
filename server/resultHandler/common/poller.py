import logging
import struct
import uuid
import os
from utils.TCPhandler import SocketBroken, TCPHandler
from utils.serializer.lineSerializer import LineSerializer
from utils.protocol import make_eof, make_wait, TlvTypes

EOF_LINE = "EOF"


class Poller():
    def __init__(self, client_sock, results_directory, directory_lock):
        self.serializer = LineSerializer()
        self.tcpHandler = TCPHandler(client_sock)
        self.results_directory = results_directory
        self.directory_lock = directory_lock

        self.keep_reading = True

        self.chunk_size = 10
        self.cursor = 0
        self.eof_readed = False

    def wait_poll(self):
        return self.tcpHandler.read(TlvTypes.SIZE_CODE_MSG)

    def ack(self):
        bytes = int.to_bytes(TlvTypes.ACK, TlvTypes.SIZE_CODE_MSG, 'big')
        result = self.tcpHandler.send_all(bytes)
        assert result == len(bytes), 'TCP Error: cannot send ACK'

    def handshake(self):
        raw = self.tcpHandler.read(TlvTypes.SIZE_CODE_MSG+TlvTypes.SIZE_UUID_MSG)
        type = struct.unpack('!i', raw[:TlvTypes.SIZE_CODE_MSG])[0]
        assert type == TlvTypes.UUID, f'Unexpected type: expected: UUID({TlvTypes.UUID}), received {type}'
        client_uuid = uuid.UUID(bytes=raw[TlvTypes.SIZE_CODE_MSG:])
        self.ack()
        return client_uuid

    def run(self, semaphore):
        try:
            self.client_id = self.handshake()
            self.file_name = self.results_directory + '/' + str(self.client_id) + '.csv'
            logging.debug(f'action: poller_run | client_id: {str(self.client_id)}')
            while self.keep_reading:
                self.wait_poll()
                chunk = self.poll()
                self.respond(chunk)

        except (SocketBroken, OSError) as e:
            if not self.eof_readed:
                logging.error(f'action: poller_run | result: fail | error: {str(e) or repr(e)}')

        self.tcpHandler.close()
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
            data = self.serializer.to_bytes(chunk)
        else:
            if self.eof_readed:
                logging.debug('action: poller_respond | response: EOF')
                data = make_eof()
            else:
                logging.debug('action: poller_respond | response: WAIT')
                data = make_wait()
        self.tcpHandler.send_all(data)

    def stop(self):
        self.keep_reading = False
