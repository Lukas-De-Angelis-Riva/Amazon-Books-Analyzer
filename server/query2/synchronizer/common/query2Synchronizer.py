import logging

from utils.worker import Worker
from utils.middleware.middleware import Middleware
from utils.serializer.q2PartialSerializer import Q2PartialSerializer    # type: ignore
from utils.serializer.q2OutSerializer import Q2OutSerializer            # type: ignore


class Query2Synchronizer(Worker):
    def __init__(self, chunk_size, min_decades):
        middleware = Middleware()
        middleware.consume(queue_name='Q2-Sync', callback=self.recv)
        super().__init__(middleware=middleware,
                         in_serializer=Q2PartialSerializer(),
                         out_serializer=Q2OutSerializer(),
                         peers=1,
                         chunk_size=chunk_size,)
        self.min_decades = min_decades

    def forward_data(self, data):
        self.middleware.publish(data, 'results', 'Q2')

    def resend(self, data):
        self.middleware.requeue(data, 'Q2-Sync')

    def work(self, input):
        partial = input
        logging.debug(f'action: new_partial | {partial}')
        author = partial.author
        if author in self.results:
            self.results[author].merge(partial)
        else:
            self.results[author] = partial

    def passes_filter(self, partial):
        return len(partial.decades) >= self.min_decades

    def send_results(self):
        chunk = []
        for partial in self.results.values():
            if not self.passes_filter(partial):
                continue
            logging.debug(f'action: publish_result | value: {partial.author}')
            chunk.append(partial.author)
            if len(chunk) >= self.chunk_size:
                data = self.out_serializer.to_bytes(chunk)
                self.forward_data(data)
                chunk = []
        if chunk:
            data = self.out_serializer.to_bytes(chunk)
            self.forward_data(data)
