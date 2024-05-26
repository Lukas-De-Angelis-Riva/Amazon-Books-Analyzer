import logging
import io

from utils.listener2 import Listener2
from utils.protocol import make_eof, get_eof_argument


class Worker2(Listener2):
    def __init__(self, middleware, in_serializer, out_serializer, peer_id, peers, chunk_size):
        super().__init__(middleware)
        self.peer_id = peer_id
        self.peers = peers
        self.chunk_size = chunk_size
        self.results = {}
        self.in_serializer = in_serializer
        self.out_serializer = out_serializer
        self.worked = 0
        self.remaining = -1
        self.received_eof = False

        self.is_leader = (self.peer_id == self.peers)

    def forward_data(self, data):
        raise RuntimeError("Must be redefined")

    def send_to_peer(self, data, peer_id):
        raise RuntimeError("Must be redefined")

    def work(self, input):
        return

    def do_after_work(self):
        return

    def recv_raw(self, raw, key):
        if not self.is_leader and self.received_eof:
            logging.debug('action: recv_raw | status: unexpected_raw | NACK')
            # TODO: return Middleware.NACK
            return 2

        reader = io.BytesIO(raw)
        input_chunk = self.in_serializer.from_chunk(reader)
        logging.debug(f'action: recv_raw | status: new_chunk | len(chunk): {len(input_chunk)}')

        for input in input_chunk:
            self.work(input)
        self.do_after_work()
        self.worked += len(input_chunk)

        if self.remaining >= 0 and self.remaining == self.worked:
            logging.debug('action: recv_raw | status: success | forwarding_eof')
            self.send_results()
            eof = make_eof(i=0)
            self.forward_data(eof)

        # TODO: return Middleware.ACK
        return True

    def send_results(self):
        chunk = []
        logging.debug(f'action: send_results | status: in_progress | len(results): {len(self.results)}')
        for result in self.results.values():
            chunk.append(result)
            if len(chunk) >= self.chunk_size:
                logging.debug(f'action: send_results | status: in_progress | forwarding_chunk | len(chunk): {len(chunk)}')
                data = self.out_serializer.to_bytes(chunk)
                self.forward_data(data)
                chunk = []
        if chunk:
            logging.debug(f'action: send_results | status: in_progress | forwarding_chunk | len(chunk): {len(chunk)}')
            data = self.out_serializer.to_bytes(chunk)
            self.forward_data(data)
        logging.debug('action: send_results | status: success')

    def recv_eof(self, eof, key):
        logging.debug('action: recv_eof | status: in_progress')
        self.send_results()

        remaining = get_eof_argument(eof)
        logging.debug(f'action: recv_eof | status: in_progress | worked: {self.worked} | remaining: {remaining-self.worked}')
        if self.is_leader:
            if remaining == self.worked:
                logging.debug('action: recv_eof | status: success | forwarding_eof ')
                eof = make_eof(i=0)
                self.forward_data(eof)
            else:
                logging.debug(f'action: recv_eof | status: waiting | total_left: {remaining-self.worked}')
                self.remaining = remaining
        else:
            logging.debug(f'action: recv_eof | status: success | sending_eof_to: {self.peer_id+1}')
            new_remaining = remaining - self.worked
            eof = make_eof(i=new_remaining)
            self.send_to_peer(data=eof, peer_id=self.peer_id+1)
        self.received_eof = True

        # TODO: return Middleware.ACK
        return True
