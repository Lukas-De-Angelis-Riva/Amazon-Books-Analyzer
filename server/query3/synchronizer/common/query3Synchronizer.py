import logging

from utils.worker2 import Worker2
from utils.middleware.middleware import Middleware
from utils.serializer.q3PartialSerializer import Q3PartialSerializer    # type: ignore
from utils.serializer.q3OutSerializer import Q3OutSerializer            # type: ignore
from utils.protocol import make_eof2, get_eof_argument2


class Query3Synchronizer(Worker2):
    def __init__(self, chunk_size, min_amount_reviews, n_top):
        middleware = Middleware()
        middleware.consume(queue_name='Q3-Sync', callback=self.recv_raw)
        middleware.subscribe(topic='Q3-EOF', tags=['SYNC'], callback=self.recv_eof)
        super().__init__(middleware=middleware,
                         in_serializer=Q3PartialSerializer(),
                         out_serializer=Q3OutSerializer(),
                         peer_id=1,
                         peers=1,
                         chunk_size=chunk_size,)
        self.min_amount_reviews = min_amount_reviews
        self.n_top = n_top

    def forward_eof(self, eof):
        self.middleware.publish(eof, 'results', 'Q3')

        total, worked, sent = get_eof_argument2(eof)
        eof = make_eof2(total=min(self.n_top, total), worked=0, sent=0)
        self.middleware.publish(eof, 'results', 'Q4')

    def forward_data(self, data):
        self.middleware.publish(data, 'results', self.tag)

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

    def filter_results(self):
        return {k: v for k, v in self.results.items() if v.n >= self.min_amount_reviews}

    def get_top(self):
        n = min(self.n_top, len(self.results))
        _sorted_keys = sorted(self.results, key=lambda k: self.results[k].scoreAvg, reverse=True)[:n]
        return {k: v for k, v in self.results.items() if k in _sorted_keys}

    def send_results(self):
        n = len(self.results)
        self.results = self.filter_results()
        logging.debug(f'action: filtering_result Q3 | result: success | n: {n} >> {len(self.results)}')

        self.tag = 'Q3'
        super().send_results()
        logging.debug('action: send_results Q3 | result: success')

        auxiliary = self.results

        self.tag = 'Q4'
        self.results = self.get_top()
        super().send_results()

        self.results = auxiliary
        logging.debug('action: send_results Q4 | result: success')
