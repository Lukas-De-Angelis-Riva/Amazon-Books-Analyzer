import logging

from utils.worker import Worker
from utils.middleware.middleware import Middleware
from utils.serializer.q3PartialSerializer import Q3PartialSerializer    # type: ignore
from utils.serializer.q3OutSerializer import Q3OutSerializer            # type: ignore


class Query3Synchronizer(Worker):
    def __init__(self, chunk_size, min_amount_reviews, n_top):
        middleware = Middleware()
        middleware.consume(queue_name='Q3-Sync', callback=self.recv)
        super().__init__(middleware=middleware,
                         in_serializer=Q3PartialSerializer(),
                         out_serializer=Q3OutSerializer(),
                         peers=1,
                         chunk_size=chunk_size,)
        self.min_amount_reviews = min_amount_reviews
        self.n_top = n_top

    def forward_data(self, data):
        self.middleware.publish(data, 'results', self.tag)

    def resend(self, data):
        self.middleware.requeue(data, 'Q3-Sync')

    def work(self, input):
        partial = input
        logging.debug(f'action: new_partial | result: merge | partial: {partial}')
        title = partial.title
        if title in self.results:
            self.results[title].merge(partial)
        else:
            self.results[title] = partial

    def recv_eof(self, eof):
        n = len(self.results)
        self.results = self.filter_results()
        logging.debug(f'action: filtering_result Q3 | result: success | n: {n} >> {len(self.results)}')

        # First send Q3 results
        self.tag = 'Q3'
        super().send_results()
        super().handle_eof(eof)
        logging.debug('action: send_results Q3 | result: success')

        # Then send Q4 results
        self.results = self.get_top()
        logging.debug(f'action: filtering_result Q4 | result: success | n: {len(self.results)}')

        self.tag = 'Q4'
        super().send_results()
        super().handle_eof(eof)
        logging.debug('action: send_results Q4 | result: success')

    def filter_results(self):
        return {k: v for k, v in self.results.items() if v.n >= self.min_amount_reviews}

    def get_top(self):
        n = min(self.n_top, len(self.results))
        _sorted_keys = sorted(self.results, key=lambda k: self.results[k].scoreAvg, reverse=True)[:n]
        return {k: v for k, v in self.results.items() if k in _sorted_keys}
