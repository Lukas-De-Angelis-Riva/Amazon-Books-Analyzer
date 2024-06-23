from utils.synchronizer import Synchronizer, TOTAL
from utils.middleware.middleware import Middleware
from utils.serializer.q2OutSerializer import Q2OutSerializer    # type: ignore
from utils.model.message import Message, MessageType

IN_QUEUE_NAME = 'Q2-Sync'
OUT_TOPIC = 'results'
TAG = 'Q2'


class Query2Synchronizer(Synchronizer):
    def __init__(self, n_workers, test_middleware=None):
        middleware = test_middleware if test_middleware else Middleware()
        middleware.consume(queue_name=IN_QUEUE_NAME, callback=self.recv)
        super().__init__(
            middleware=middleware,
            n_workers=n_workers,
            in_serializer=Q2OutSerializer(),
            out_serializer=Q2OutSerializer(),
            # This synchronizer doesn't aggregate, so chunk_size does not matter.
            # Also, it is not used in 'Synchronizer' abstraction. So might be deleted
            chunk_size=1
        )
        self.recovery()

    def process_chunk(self, chunk, chunk_id):
        data = self.out_serializer.to_bytes(chunk)
        msg = Message(
            client_id=self.tracker.client_id,
            type=MessageType.DATA,
            data=data
        )
        msg.ID = chunk_id
        self.middleware.publish(msg.to_bytes(), OUT_TOPIC, TAG)

    def terminator(self):
        eof = Message(
            client_id=self.tracker.client_id,
            type=MessageType.EOF,
            data=b'',
            args={
                TOTAL: self.tracker.total_worked()
            }
        )
        eof.ID = self.tracker.eof_id()
        self.middleware.publish(eof.to_bytes(), OUT_TOPIC, TAG)
