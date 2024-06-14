from utils.synchronizer import Synchronizer, TOTAL
from utils.middleware.middleware import Middleware
from utils.serializer.q1OutSerializer import Q1OutSerializer        # type: ignore
from utils.model.message import Message, MessageType

IN_QUEUE_NAME = 'Q1-Sync'
OUT_TOPIC = 'results'
TAG = 'Q1'


class Query1Synchronizer(Synchronizer):
    def __init__(self, n_workers, test_middleware=None):
        if test_middleware:
            middleware = test_middleware
        else:
            middleware = Middleware()
        middleware.consume(queue_name=IN_QUEUE_NAME, callback=self.recv)
        super().__init__(
            middleware=middleware,
            n_workers=n_workers,
            in_serializer=Q1OutSerializer(),
            out_serializer=Q1OutSerializer(),
            # This synchronizer doesn't aggregate, so chunk_size does not matter.
            # Also, it is not used in 'Synchronizer' abstraction. So might be deleted
            chunk_size=1
        )

    def process_chunk(self, chunk, client_id):
        data = self.out_serializer.to_bytes(chunk)
        msg = Message(
            client_id=client_id,
            type=MessageType.DATA,
            data=data
        )
        self.middleware.publish(msg.to_bytes(), OUT_TOPIC, TAG)

    def terminator(self, client_id):
        eof = Message(
            client_id=client_id,
            type=MessageType.EOF,
            data=b'',
            args={
                TOTAL: sum(self.total_by_worker.values())
            }
        )
        self.middleware.publish(eof.to_bytes(), OUT_TOPIC, TAG)
