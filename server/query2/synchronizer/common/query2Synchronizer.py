import logging

from utils.synchronizer import Synchronizer
from utils.middleware.middleware import Middleware
from utils.serializer.q2PartialSerializer import Q2PartialSerializer    # type: ignore
from utils.serializer.q2OutSerializer import Q2OutSerializer            # type: ignore

IN_QUEUE_NAME = 'Q2-Sync'
OUT_TOPIC = 'results'
TAG = 'Q2'


class Query2Synchronizer(Synchronizer):
    def __init__(self, n_workers, min_decades):
        middleware = Middleware()
        middleware.consume(queue_name=IN_QUEUE_NAME, callback=self.recv)
        super().__init__(
            middleware=middleware,
            n_workers=n_workers,
            in_serializer=Q2PartialSerializer(),
            out_serializer=Q2OutSerializer(),
            # This synchronizer doesn't aggregate, so chunk_size does not matter.
            # Also, it is not used in 'Synchronizer' abstraction. So might be deleted
            chunk_size=1
        )
        self.min_decades = min_decades

    def forward_eof(self, eof):
        self.middleware.publish(eof, OUT_TOPIC, TAG)

    def forward_data(self, data):
        self.middleware.publish(data, OUT_TOPIC, TAG)

    # Dummy synchronizer, just forwards chunks
    def process_chunk(self, chunk):
        data = self.out_serializer.to_bytes(chunk)
        self.forward_data(data)
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
