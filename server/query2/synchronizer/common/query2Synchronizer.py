import logging

from utils.worker import Worker
from utils.middleware.middlewareQE import MiddlewareQE
from utils.serializer.partialQ2Serializer import PartialQ2Serializer
from utils.serializer.resultQ2Serializer import ResultQ2Serializer

class Query2Synchronizer(Worker):
    def __init__(self):
        middleware = MiddlewareQE(in_queue_name='Q2-Sync',
                                  exchange='results',
                                  tag='Q2')
        super().__init__(middleware=middleware,
                         in_serializer=PartialQ2Serializer(),
                         out_serializer=ResultQ2Serializer(),
                         peers=1,
                         chunk_size=1,)

    def work(self, input):
        partial = input
        logging.info(f'action: new_partial | {partial}')
        author = partial.author
        if author in self.results:
            self.results[author].merge(partial)
        else:
            self.results[author] = partial

    def passes_filter(self, partial):
        return len(partial.decades) >= 3

    def send_results(self):
        chunk = []
        for partial in self.results.values():
            if not self.passes_filter(partial):
                continue
            
            logging.info(f'action: publish_result | value: {partial.author}')
            chunk.append(partial.author)
            if len(chunk) >= self.chunk_size:
                data = self.out_serializer.to_bytes(chunk)
                self.middleware.publish(data)
                chunk = []
        if chunk:
            data = self.out_serializer.to_bytes(chunk)
            self.middleware.publish(data)
