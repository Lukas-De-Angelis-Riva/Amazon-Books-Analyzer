import logging
from math import ceil

from utils.synchronizer import Synchronizer, TOTAL
from utils.middleware.middleware import Middleware
from utils.serializer.q5PartialSerializer import Q5PartialSerializer    # type: ignore
from utils.serializer.q5OutSerializer import Q5OutSerializer            # type: ignore
from dto.q5Partial import Q5Partial
from utils.model.message import Message, MessageType

IN_QUEUE_NAME = 'Q5-Sync'
OUT_TOPIC = 'results'
TAG = 'Q5'


class Query5Synchronizer(Synchronizer):
    def __init__(self, n_workers, chunk_size, percentage, test_middleware=None):
        middleware = test_middleware if test_middleware else Middleware()
        middleware.consume(queue_name=IN_QUEUE_NAME, callback=self.recv)
        super().__init__(middleware=middleware,
                         n_workers=n_workers,
                         in_serializer=Q5PartialSerializer(),
                         out_serializer=Q5OutSerializer(),
                         chunk_size=chunk_size,)
        self.percentage = percentage
        self.recovery()

    def adapt_tracker(self):
        self.tracker.parser = Q5Partial.decode

    def process_chunk(self, chunk, chunk_id):
        for result in chunk:
            logging.debug(f'action: new_result | result: {result}')
            self.tracker.data[result.title] = result

    def get_percentile(self):
        values = [v.sentimentAvg for v in self.tracker.data.values()]
        i = ceil(len(values) * self.percentage/100) - 1
        return sorted(values)[i]

    def filter_results(self):
        if len(self.tracker.data) == 0:
            return []
        percentile = self.get_percentile()
        logging.debug(f'action: filtering_result | result: in_progress | percentile: {percentile}')
        results = [v.title for v in self.tracker.data.values() if v.sentimentAvg >= percentile]
        results.sort()
        return results

    def send_chunk(self, chunk, chunk_id):
        logging.debug(f'action: send_results | status: in_progress | forwarding_chunk | len(chunk): {len(chunk)}')
        data = self.out_serializer.to_bytes(chunk)
        msg = Message(
            client_id=self.tracker.client_id,
            type=MessageType.DATA,
            data=data
        )
        msg.ID = chunk_id
        self.middleware.publish(msg.to_bytes(), topic=OUT_TOPIC, tag=TAG)
        return

    def send_results(self, results):
        chunk = []
        id_iterator = iter(self.tracker.worked_chunks)
        logging.debug(f'action: send_results | status: in_progress | len(results): {len(results)}')
        for result in results:
            chunk.append(result)
            if len(chunk) >= self.chunk_size:
                self.send_chunk(chunk, next(id_iterator))
                chunk = []
        if chunk:
            self.send_chunk(chunk, next(id_iterator))
        logging.debug('action: send_results | status: success')
        return

    def send_eof(self, sent):
        eof = Message(
            client_id=self.tracker.client_id,
            type=MessageType.EOF,
            data=b'',
            args={
                TOTAL: sent
            }
        )
        eof.ID = self.tracker.eof_id()
        self.middleware.publish(eof.to_bytes(), topic=OUT_TOPIC, tag=TAG)
        return

    def terminator(self):
        results = self.filter_results()
        logging.debug(f'action: filtering_result | result: success | n: {len(self.tracker.data)} >> {len(results)}')
        self.send_results(results)
        self.send_eof(len(results))
