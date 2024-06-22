import logging
from math import ceil

from utils.synchronizer import Synchronizer, ClientTracker, TOTAL
from utils.middleware.middleware import Middleware
from utils.serializer.q5PartialSerializer import Q5PartialSerializer    # type: ignore
from utils.serializer.q5OutSerializer import Q5OutSerializer            # type: ignore
from utils.model.message import Message, MessageType

IN_QUEUE_NAME = 'Q5-Sync'
OUT_TOPIC = 'results'
TAG = 'Q5'


class ClientTrackerWithResults(ClientTracker):
    def __init__(self, client_id, n_workers):
        super().__init__(client_id, n_workers)
        self.results = {}


class Query5Synchronizer(Synchronizer):
    def __init__(self, n_workers, chunk_size, percentage):
        middleware = Middleware()
        middleware.consume(queue_name=IN_QUEUE_NAME, callback=self.recv)
        super().__init__(middleware=middleware,
                         n_workers=n_workers,
                         in_serializer=Q5PartialSerializer(),
                         out_serializer=Q5OutSerializer(),
                         chunk_size=chunk_size,)
        self.percentage = percentage

    def recv(self, raw_msg, key):
        msg = Message.from_bytes(raw_msg)
        if msg.client_id not in self.clients:
            self.clients[msg.client_id] = ClientTrackerWithResults(msg.client_id, self.n_workers)
        self.tracker = self.clients[msg.client_id]

        return super().recv(raw_msg, key)

    def process_chunk(self, chunk, chunk_id):
        for result in chunk:
            logging.debug(f'action: new_result | result: {result}')
            self.tracker.results[result.title] = result

    def get_percentile(self):
        values = [v.sentimentAvg for v in self.tracker.results.values()]
        i = ceil(len(values) * self.percentage/100) - 1
        return sorted(values)[i]

    def filter_results(self):
        percentile = self.get_percentile()
        logging.debug(f'action: filtering_result | result: in_progress | percentile: {percentile}')
        return {k: v.title for k, v in self.tracker.results.items() if v.sentimentAvg >= percentile}

    def send_chunk(self, chunk):
        logging.debug(f'action: send_results | status: in_progress | forwarding_chunk | len(chunk): {len(chunk)}')
        data = self.out_serializer.to_bytes(chunk)
        msg = Message(
            client_id=self.tracker.client_id,
            type=MessageType.DATA,
            data=data
        )
        self.middleware.publish(msg.to_bytes(), topic=OUT_TOPIC, tag=TAG)
        return

    def send_results(self):
        chunk = []
        logging.debug(f'action: send_results | status: in_progress | len(results): {len(self.tracker.results)}')
        for result in self.tracker.results.values():
            chunk.append(result)
            if len(chunk) >= self.chunk_size:
                self.send_chunk(chunk)
                chunk = []
        if chunk:
            self.send_chunk(chunk)
        logging.debug('action: send_results | status: success')
        return

    def send_eof(self):
        eof = Message(
            client_id=self.tracker.client_id,
            type=MessageType.EOF,
            data=b'',
            args={
                TOTAL: len(self.tracker.results)
            }
        )
        eof.ID = self.tracker.eof_id()
        self.middleware.publish(eof.to_bytes(), topic=OUT_TOPIC, tag=TAG)
        return

    def terminator(self):
        n = len(self.tracker.results)
        if n > 0:
            self.tracker.results = self.filter_results()
            logging.debug(f'action: filtering_result | result: success | n: {n} >> {len(self.tracker.results)}')
            self.send_results()
        self.send_eof()
