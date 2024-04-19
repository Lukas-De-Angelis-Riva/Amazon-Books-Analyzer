import logging

from utils.worker import Worker
from utils.middleware.middlewareQE import MiddlewareQE
from utils.serializer.partialQ3Serializer import PartialQ3Serializer
from utils.serializer.resultQ3Serializer import ResultQ3Serializer

class Query3Synchronizer(Worker):
    def __init__(self, chunk_size, min_amount_reviews, n_top):
        middleware = MiddlewareQE(in_queue_name='Q3-Sync',
                                  exchange='results',
                                  tag='Q3')
        super().__init__(middleware=middleware,
                         in_serializer=PartialQ3Serializer(),
                         out_serializer=ResultQ3Serializer(),
                         peers=1,
                         chunk_size=chunk_size,)
        self.min_amount_reviews = min_amount_reviews
        self.n_top = n_top

    def work(self, input):
        partial = input
        logging.info(f'action: new_partial | {partial}')
        title = partial.title
        if title in self.results:
            self.results[title].merge(partial)
        else:
            self.results[title] = partial

    def send_results(self):
        self.results = self.filter_results()

        # First send Q3 results
        super().send_results()
        
        # Then send Q4 results
        self.middleware.change_tag('Q4')
        top = self.get_top()
        data = self.out_serializer.to_bytes(top)
        self.middleware.publish(data)

    def filter_results(self):
        return {k:v for k, v in self.results.items() if v.n >= self.min_amount_reviews}

    def get_top(self):
        n = min(self.n_top, len(self.results))
        _sorted_keys = sorted(self.results, key=lambda k: self.results[k].scoreAvg, reverse=True)[:n]
        return [self.results[key] for key in _sorted_keys]