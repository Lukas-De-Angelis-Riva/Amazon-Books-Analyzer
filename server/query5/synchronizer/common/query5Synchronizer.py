import logging
from math import ceil

from utils.worker import Worker
from utils.middleware.middleware import Middleware
from utils.serializer.q5PartialSerializer import Q5PartialSerializer    # type: ignore
from utils.serializer.q5OutSerializer import Q5OutSerializer            # type: ignore


class Query5Synchronizer(Worker):
    def __init__(self, chunk_size, percentage):
        middleware = Middleware()
        middleware.consume(queue_name='Q5-Sync', callback=self.recv_raw)
        middleware.subscribe(topic='Q5-EOF', tags=['SYNC'], callback=self.recv_eof)
        super().__init__(middleware=middleware,
                         in_serializer=Q5PartialSerializer(),
                         out_serializer=Q5OutSerializer(),
                         peer_id=1,
                         peers=1,
                         chunk_size=chunk_size,)
        self.percentage = percentage

    def forward_eof(self, eof):
        self.middleware.publish(eof, 'results', 'Q5')

    def forward_data(self, data):
        self.middleware.publish(data, 'results', 'Q5')

    def send_to_peer(self, data, peer_id):
        return

    def work(self, input):
        partial = input
        logging.debug(f'action: new_partial | result: merge | partial: {partial}')
        title = partial.title
        if title in self.results:
            self.results[title].merge(partial)
        else:
            self.results[title] = partial

    def do_after_work(self):
        return

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

    def send_results(self):
        n = len(self.results)
        if n > 0:
            self.results = self.filter_results()
            logging.debug(f'action: filtering_result | result: success | n: {n} >> {len(self.results)}')
        super().send_results()
