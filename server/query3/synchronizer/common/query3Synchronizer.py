import logging

from utils.synchronizer import Synchronizer, TOTAL
from utils.middleware.middleware import Middleware
from utils.serializer.q3PartialSerializer import Q3PartialSerializer    # type: ignore
from utils.serializer.q3OutSerializer import Q3OutSerializer            # type: ignore
from utils.model.message import Message, MessageType


IN_QUEUE_NAME = 'Q3-Sync'
OUT_TOPIC = "results"
Q3_TAG = "Q3"
Q4_TAG = "Q4"


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
        self.results = {}   # TODO: replace by persistent implementation

    def process_chunk(self, chunk, client_id):
        data = self.out_serializer.to_bytes(chunk)
        msg = Message(
            client_id=client_id,
            type=MessageType.DATA,
            data=data,
        )
        self.middleware.publish(msg.to_bytes(), OUT_TOPIC, Q3_TAG)

        for partial in chunk:
            logging.debug(f'action: new_partial | result: merge | partial: {partial}')
            title = partial.title
            if title in self.results:
                self.results[title].merge(partial)
            else:
                self.results[title] = partial

    def get_top(self, client_id):
        n = min(self.n_top, len(self.results))
        _sorted_keys = sorted(self.results, key=lambda k: self.results[k].scoreAvg, reverse=True)[:n]
        return [v for k, v in self.results.items() if k in _sorted_keys]

    def terminator(self, client_id):
        eof = Message(
            client_id=client_id,
            type=MessageType.EOF,
            data=b'',
            args={
                TOTAL: sum(self.total_by_worker.values()),
            },
        )
        self.middleware.publish(eof.to_bytes(), OUT_TOPIC, Q3_TAG)

        top = self.get_top(client_id)
        data = self.out_serializer.to_bytes(top)
        msg = Message(
            client_id=client_id,
            type=MessageType.DATA,
            data=data
        )
        self.middleware.publish(msg.to_bytes(), OUT_TOPIC, Q4_TAG)
        eof.args[TOTAL] = len(top)
        self.middleware.publish(eof.to_bytes(), OUT_TOPIC, Q4_TAG)
