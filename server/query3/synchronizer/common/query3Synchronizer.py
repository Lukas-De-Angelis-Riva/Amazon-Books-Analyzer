import logging
import uuid

from utils.synchronizer import Synchronizer, TOTAL
from utils.middleware.middleware import Middleware
from utils.serializer.q3PartialSerializer import Q3PartialSerializer    # type: ignore
from utils.serializer.q3OutSerializer import Q3OutSerializer            # type: ignore
from dto.q3Partial import Q3Partial
from utils.model.message import Message, MessageType

IN_QUEUE_NAME = 'Q3-Sync'
OUT_TOPIC = "results"
Q3_TAG = "Q3"
Q4_TAG = "Q4"
Q4_EOF_ID = "Q4_EOF_ID"


class Query3Synchronizer(Synchronizer):
    def __init__(self, n_workers, chunk_size, n_top, test_middleware=None):
        middleware = test_middleware if test_middleware else Middleware()
        middleware.consume(queue_name=IN_QUEUE_NAME, callback=self.recv)
        super().__init__(middleware=middleware,
                         n_workers=n_workers,
                         in_serializer=Q3PartialSerializer(),
                         out_serializer=Q3OutSerializer(),
                         chunk_size=chunk_size,)
        self.n_top = n_top
        self.recovery()

    def adapt_tracker(self):
        self.tracker.parser = Q3Partial.decode
        self.tracker.meta_data[Q4_EOF_ID] = str(uuid.uuid4())

    def process_chunk(self, chunk, chunk_id):
        data = self.out_serializer.to_bytes(chunk)
        msg = Message(
            client_id=self.tracker.client_id,
            type=MessageType.DATA,
            data=data,
            ID=chunk_id
        )
        self.middleware.publish(msg.to_bytes(), OUT_TOPIC, Q3_TAG)

        for result in chunk:
            logging.debug(f'action: new_result | result: {result}')
            self.tracker.data[result.title] = result

    def get_top(self):
        n = min(self.n_top, len(self.tracker.data))
        _sorted_keys = sorted(self.tracker.data, key=lambda k: self.tracker.data[k].scoreAvg, reverse=True)[:n]
        return [v for k, v in self.tracker.data.items() if k in _sorted_keys]

    def terminator(self):
        eof = Message(
            client_id=self.tracker.client_id,
            type=MessageType.EOF,
            data=b'',
            args={
                TOTAL: self.tracker.total_worked(),
            },
            ID=self.tracker.eof_id()
        )
        self.middleware.publish(eof.to_bytes(), OUT_TOPIC, Q3_TAG)

        top = self.get_top()
        data = self.out_serializer.to_bytes(top)
        msg = Message(
            client_id=self.tracker.client_id,
            type=MessageType.DATA,
            data=data,
            # mimic first chunk_id sent, but with Q4_TAG
            ID=self.tracker.worked_chunks[0][0]
        )
        self.middleware.publish(msg.to_bytes(), OUT_TOPIC, Q4_TAG)
        eof.ID = uuid.UUID(self.tracker.meta_data[Q4_EOF_ID])
        eof.args[TOTAL] = len(top)
        self.middleware.publish(eof.to_bytes(), OUT_TOPIC, Q4_TAG)
