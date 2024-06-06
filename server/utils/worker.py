import logging
import io

from utils.listener import Listener
from utils.model.message import Message, MessageType
from utils.model.token import Token

REMAINING = "remaining"
CLI_ID = "client_id"
SENT = "sent"


class Worker(Listener):
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
        self.is_leader = False

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
        msg = Message.from_bytes(raw)
        # get cliend_ID
        if msg.type == MessageType.EOF:
            total = int.from_bytes(msg.data, 'big')
            return self.recv_eof(total, msg.client_id)

        if not self.is_leader and self.received_eof:
            logging.debug('action: recv_raw | status: unexpected_raw | NACK')
            # TODO: return Middleware.NACK
            return 2

        reader = io.BytesIO(msg.data)
        input_chunk = self.in_serializer.from_chunk(reader)
        logging.debug(f'action: recv_raw | status: new_chunk | len(chunk): {len(input_chunk)}')

        for input in input_chunk:
            self.work(input)
        self.do_after_work()
        self.worked += len(input_chunk)
        self.remaining -= len(input_chunk)

        if self.remaining == 0:
            self.send_results(client_id=...)
            sent = self.total_sent + len(self.results)
            eof = Message(
                client_id=...,
                type=MessageType.EOF,
                data=sent.to_bytes(length=4),
            )
            # forward to next stage
            self.forward_eof(eof)

        # TODO: return Middleware.ACK
        return True

    def send_results(self, client_id):
        chunk = []
        logging.debug(f'action: send_results | status: in_progress | len(results): {len(self.results)}')
        for result in self.results.values():
            chunk.append(result)
            if len(chunk) >= self.chunk_size:
                logging.debug(f'action: send_results | status: in_progress | forwarding_chunk | len(chunk): {len(chunk)}')
                data = self.out_serializer.to_bytes(chunk)
                msg = Message(
                    client_id=client_id,
                    type=MessageType.DATA,
                    data=data
                )
                self.forward_data(msg.to_bytes())
                chunk = []
        if chunk:
            logging.debug(f'action: send_results | status: in_progress | forwarding_chunk | len(chunk): {len(chunk)}')
            data = self.out_serializer.to_bytes(chunk)
            msg = Message(
                client_id=client_id,
                type=MessageType.DATA,
                data=data
            )
            self.forward_data(msg.to_bytes())
        logging.debug('action: send_results | status: success')

    def recv_eof(self, total, client_id):
        logging.debug('action: recv_eof_from_client_handler | status: success')
        prev_peer_id = ((self.peer_id - 2) % self.peers) + 1
        token = Token(
            leader=prev_peer_id,
            metadata={
                CLI_ID: client_id,
                REMAINING: total,
                SENT: 0,
            },
        )
        return self.recv_token(token.to_bytes(), key=str(self.peer_id))

    def recv_token(self, raw, key):
        logging.debug('action: recv_eof_from_peer | status: in_progress | worked: {self.worked}')
        token = Token.from_bytes(raw)
        token.metadata[REMAINING] -= self.worked

        if self.peer_id == token.leader and token.metadata[REMAINING] > 0:
            # leader must wait
            logging.debug(f'action: recv_eof | status: waiting | total_left: {token.metadata[REMAINING]}')
            self.total_sent = token.metadata[SENT]
            self.remaining = token.metadata[REMAINING]
            self.is_leader = True

        elif token.metadata[REMAINING] <= 0:
            # anyone can forward eof
            self.send_results(token.metadata[CLI_ID])
            sent = token.metadata[SENT] + len(self.results)

            eof = Message(
                client_id=token.metadata[CLI_ID],
                type=MessageType.EOF,
                data=sent.to_bytes(length=4),
            )
            # forward to next stage
            self.forward_eof(eof)
            logging.debug('action: recv_eof_from_peer | status: success | forwarding_eof')
        else:
            # remaining data and not the leader, then ring token
            self.send_results(token.metadata[CLI_ID])
            next_id = (self.peer_id % self.peers) + 1
            logging.debug(f'action: recv_eof_from_peer | status: success | sending_eof_to: {next_id}')
            new_token = Token(
                leader=token.leader,
                metadata={
                    CLI_ID: token.metadata[CLI_ID],
                    REMAINING: token.metadata[REMAINING],
                    SENT: token.metadata[SENT] + len(self.results),
                },
            )
            self.send_to_peer(data=new_token.to_bytes(), peer_id=next_id)

        self.received_eof = True

        # TODO: return Middleware.ACK
        return True
