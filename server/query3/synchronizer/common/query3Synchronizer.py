import logging

from utils.clientTrackerSynchronizer import ClientTrackerSynchronizer
from utils.synchronizer import Synchronizer, TOTAL
from utils.middleware.middleware import Middleware
from utils.serializer.q3PartialSerializer import Q3PartialSerializer    # type: ignore
from utils.serializer.q3OutSerializer import Q3OutSerializer            # type: ignore
from utils.model.message import Message, MessageType


IN_QUEUE_NAME = 'Q3-Sync'
OUT_TOPIC = "results"
Q3_TAG = "Q3"
Q4_TAG = "Q4"


class ClientTrackerWithResults(ClientTrackerSynchronizer):
    def __init__(self, client_id, n_workers):
        super().__init__(client_id, n_workers)
        self.results = {}


class Query3Synchronizer(Synchronizer):
    def __init__(self, n_workers, chunk_size, n_top):
        middleware = Middleware()
        middleware.consume(queue_name=IN_QUEUE_NAME, callback=self.recv)
        super().__init__(middleware=middleware,
                         n_workers=n_workers,
                         in_serializer=Q3PartialSerializer(),
                         out_serializer=Q3OutSerializer(),
                         chunk_size=chunk_size,)
        self.n_top = n_top
        self.recovery()

    def recv(self, raw_msg, key):
        msg = Message.from_bytes(raw_msg)
        if msg.client_id not in self.clients:
            self.clients[msg.client_id] = ClientTrackerWithResults(msg.client_id, self.n_workers)
        self.tracker = self.clients[msg.client_id]

        return super().recv(raw_msg, key)

    def process_chunk(self, chunk, chunk_id):
        data = self.out_serializer.to_bytes(chunk)
        msg = Message(
            client_id=self.tracker.client_id,
            type=MessageType.DATA,
            data=data,
        )
        msg.ID = chunk_id
        self.middleware.publish(msg.to_bytes(), OUT_TOPIC, Q3_TAG)

        for result in chunk:
            logging.debug(f'action: new_result | result: {result}')
            self.tracker.results[result.title] = result

    def get_top(self):
        n = min(self.n_top, len(self.tracker.results))
        _sorted_keys = sorted(self.tracker.results, key=lambda k: self.tracker.results[k].scoreAvg, reverse=True)[:n]
        return [v for k, v in self.tracker.results.items() if k in _sorted_keys]

    def terminator(self):
        eof = Message(
            client_id=self.tracker.client_id,
            type=MessageType.EOF,
            data=b'',
            args={
                TOTAL: self.tracker.total_worked(),
            },
        )
        eof.ID = self.tracker.eof_id()
        self.middleware.publish(eof.to_bytes(), OUT_TOPIC, Q3_TAG)

        top = self.get_top()
        data = self.out_serializer.to_bytes(top)
        msg = Message(
            client_id=self.tracker.client_id,
            type=MessageType.DATA,
            data=data
        )
        # TODO: another EOF_ID2
        # CHANGE ID, TO IDEMPOTENCY !!!
        # eof.ID = self.tracker.eof_id2(), special only in this class
        self.middleware.publish(msg.to_bytes(), OUT_TOPIC, Q4_TAG)
        eof.args[TOTAL] = len(top)
        self.middleware.publish(eof.to_bytes(), OUT_TOPIC, Q4_TAG)
