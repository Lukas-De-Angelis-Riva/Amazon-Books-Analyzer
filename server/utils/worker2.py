import logging
import io

from utils.listener2 import Listener2
from utils.protocol import make_eof2, get_eof_argument2


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
        self.total_sent = 0
        self.is_leader = (self.peer_id == self.peers)

    def forward_eof(self, eof):
        raise RuntimeError("Must be redefined")

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
            self.send_results()
            logging.debug('action: recv_eof | status: success | forwarding_eof')
            eof = make_eof2(total=len(self.results)+self.total_sent, worked=0, sent=0)
            self.forward_eof(eof)

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

    def recv_eof(self, raw, key):
        total = get_eof_argument(raw)
        logging.debug(f'action: recv_eof | status: success | total: {total}')

        token = make_token(peer_id=self.peer_id, total=total, worked=0, sent=0)
        next_id = self.peer_id + 1 if self.peer_id != self.peers else 1 # ids start on 1
        send_to_peer(data=token, peer_id=next_id)
        logging.debug(f'action: send_token | status: success | sent_to: {next_id}')


    def recv_token(self, raw, key):
        peer_id, total, worked, sent = get_token_args(raw)
        worked += self.worked
        logging.debug(f'action: recv_eof_from_peer | status: in_progress | worked: {worked} | remaining: {total - worked}')
        
        # leader is the one that closes the ring,
        # i.e. counter clockwise neighbor
        prev_id = peer_id - 1 if peer_id >= 2 else self.peers
        is_leader = self.peer_id == prev_id

        if is_leader:
            if worked >= total:
                self.send_results()
                sent += len(self.results)
                eof = make_eof2(total=len(self.results)+sent, worked=0, sent=0)
                self.forward_eof(eof)
                logging.debug('action: recv_eof_from_peer | status: success | forwarding_eof')
            else:
                logging.debug(f'action: recv_eof_from_peer | status: waiting | total_left: {total-worked}')
                self.total_sent = sent
                self.remaining = total - worked 
        else:
            self.send_results()
            sent += len(self.results)

            logging.debug(f'action: recv_eof | status: success | sending_eof_to: {self.peer_id+1}')
            token = make_token(peer_id=peer_id, total=total, worked=worked, sent=sent)
            next_id = self.peer_id + 1 if self.peer_id != self.peers else 1 # ids start on 1
            self.send_to_peer(data=token, peer_id=next_id)
        self.received_eof = True

        # TODO: return Middleware.ACK
        return True

