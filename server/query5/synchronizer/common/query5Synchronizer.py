import logging
from math import ceil

from utils.synchronizer import Synchronizer, TOTAL
from utils.middleware.middleware import Middleware
from utils.serializer.q5PartialSerializer import Q5PartialSerializer    # type: ignore
from utils.serializer.q5OutSerializer import Q5OutSerializer            # type: ignore
from utils.model.message import Message, MessageType

IN_QUEUE_NAME = 'Q5-Sync'
OUT_TOPIC = 'results'
TAG = 'Q5'


class Query5Synchronizer(Synchronizer):
    def __init__(self, n_workers, chunk_size, percentage):
        middleware = Middleware()
        middleware.consume(queue_name=IN_QUEUE_NAME, callback=self.recv)
        super().__init__(middleware=middleware,
                         n_workers=n_workers,
                         in_serializer=Q5PartialSerializer(),
                         out_serializer=Q5OutSerializer(),
                         chunk_size=chunk_size,)
        self.results = {}
        self.percentage = percentage

    def process_chunk(self, chunk, client_id):
        for partial in chunk:
            logging.debug(f'action: new_partial | result: merge | partial: {partial}')
            title = partial.title
            if title in self.results:
                self.results[title].merge(partial)
            else:
                self.results[title] = partial

    def passes_filter(self, partial, percentile):
        return partial.sentimentAvg >= percentile

    def get_percentile(self):
        values = [v.sentimentAvg for v in self.results.values()]
        i = ceil(len(values) * self.percentage/100) - 1
        return sorted(values)[i]

    def filter_results(self):
        percentile = self.get_percentile()
        logging.debug(f'action: filtering_result | result: in_progress | percentile: {percentile}')
        return {k: v.title for k, v in self.results.items() if v.sentimentAvg >= percentile}

    def send_chunk(self, chunk, client_id):
        logging.debug(f'action: send_results | status: in_progress | forwarding_chunk | len(chunk): {len(chunk)}')
        data = self.out_serializer.to_bytes(chunk)
        msg = Message(
            client_id=client_id,
            type=MessageType.DATA,
            data=data
        )
        self.middleware.publish(msg.to_bytes(), topic=OUT_TOPIC, tag=TAG)
        return

    def send_results(self, client_id):
        chunk = []
        logging.debug(f'action: send_results | status: in_progress | len(results): {len(self.results)}')
        for result in self.results.values():
            chunk.append(result)
            if len(chunk) >= self.chunk_size:
                self.send_chunk(chunk, client_id)
                chunk = []
        if chunk:
            self.send_chunk(chunk, client_id)
        logging.debug('action: send_results | status: success')
        return

    def send_eof(self, client_id):
        eof = Message(
            client_id=client_id,
            type=MessageType.EOF,
            data=b'',
            args={
                TOTAL: len(self.results)
            }
        )
        self.middleware.publish(eof.to_bytes(), topic=OUT_TOPIC, tag=TAG)
        return

    def terminator(self, client_id):
        n = len(self.results)
        if n > 0:
            self.results = self.filter_results()
            logging.debug(f'action: filtering_result | result: success | n: {n} >> {len(self.results)}')
            self.send_results(client_id)
        self.send_eof(client_id)
