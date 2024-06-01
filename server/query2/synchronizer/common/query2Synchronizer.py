import logging

from utils.worker import Worker
from utils.middleware.middleware import Middleware
from utils.serializer.q2PartialSerializer import Q2PartialSerializer    # type: ignore
from utils.serializer.q2OutSerializer import Q2OutSerializer            # type: ignore


class Query2Synchronizer(Worker):
    def __init__(self, chunk_size, min_decades):
        middleware = Middleware()
        middleware.consume(queue_name='Q2-Sync', callback=self.recv_raw)
        middleware.subscribe(topic='Q2-EOF', tags=['SYNC'], callback=self.recv_eof)
        super().__init__(middleware=middleware,
                         in_serializer=Q2PartialSerializer(),
                         out_serializer=Q2OutSerializer(),
                         peer_id=1,
                         peers=1,
                         chunk_size=chunk_size,)
        self.min_decades = min_decades

    def forward_eof(self, eof):
        self.middleware.publish(eof, 'results', 'Q2')

    def forward_data(self, data):
        self.middleware.publish(data, 'results', 'Q2')

    def send_to_peer(self, data, peer_id):
        return

    def work(self, input):
        partial = input
        logging.debug(f'action: new_partial | {partial}')
        author = partial.author
        if author in self.results:
            self.results[author].merge(partial)
        else:
            self.results[author] = partial

    def do_after_work(self):
        return

    def filter_results(self):
        return {k: v.author for k, v in self.results.items() if len(v.decades) >= self.min_decades}

    def send_results(self):
        n = len(self.results)
        self.results = self.filter_results()
        logging.debug(f'action: filtering_result | result: success | n: {n} >> {len(self.results)}')
        return super().send_results()
