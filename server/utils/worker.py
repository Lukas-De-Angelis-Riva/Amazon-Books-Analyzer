import logging
import io

from utils.listener import Listener
from utils.protocol import get_closed_peers, make_eof


class Worker(Listener):
    def __init__(self, middleware, in_serializer, out_serializer, peers, chunk_size):
        super().__init__(middleware)
        self.results = {}
        self.in_serializer = in_serializer
        self.out_serializer = out_serializer
        self.peers = peers
        self.chunk_size = chunk_size

    def do_after_work(self):
        return

    def work(self, input):
        return

    def recv_raw(self, raw):
        reader = io.BytesIO(raw)
        input_chunk = self.in_serializer.from_chunk(reader)
        logging.debug(f'action: new_chunk | chunck_len: {len(input_chunk)}')

        for input in input_chunk:
            self.work(input)
        self.do_after_work()

    def recv_eof(self, eof):
        self.send_results()
        self.handle_eof(eof)

    def forward_data(self, data):
        raise RuntimeError("Must be redefined")

    def resend(self, data):
        return

    def send_results(self):
        chunk = []
        for result in self.results.values():
            logging.debug(f'action: publish_result | value: {result}')
            chunk.append(result)
            if len(chunk) >= self.chunk_size:
                data = self.out_serializer.to_bytes(chunk)
                self.forward_data(data)
                chunk = []
        if chunk:
            data = self.out_serializer.to_bytes(chunk)
            self.forward_data(data)

    def handle_eof(self, eof):
        closed_peers = get_closed_peers(eof)
        if closed_peers == -1:
            logging.error('action: close | result: fail | e = Error to parse eof')

        if closed_peers < self.peers - 1:
            # Send EOF to other peers.
            logging.debug(f'action: recv EOF | result: in_progress | peers = {self.peers} | closed_peers: {closed_peers}')
            new_eof = make_eof(closed_peers + 1)
            self.resend(new_eof)
        else:
            # All my peers are closed, send EOF to ResultQueue
            logging.debug(f'action: recv EOF | result: in_progress | peers = {self.peers} | closed_peers: {closed_peers}')
            last_eof = make_eof(0)
            self.forward_data(last_eof)
