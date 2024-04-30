import logging

from utils.worker import Worker
from utils.middleware.middlewareQQ import MiddlewareQQ
from utils.serializer.q5ReviewInSerializer import Q5ReviewInSerializer
from utils.serializer.q5PartialSerializer import Q5PartialSerializer

class Query5Worker(Worker):
    def __init__(self, peers, chunk_size, books):
        middleware = MiddlewareQQ(in_queue_name='Q5-Reviews',
                                  out_queue_name='Q5-Sync')
        super().__init__(middleware=middleware,
                         in_serializer=Q5ReviewInSerializer(),
                         out_serializer=Q5PartialSerializer(),
                         peers=peers,
                         chunk_size=chunk_size,)
        self.results = books

    def work(self, input):
        review = input
        logging.info(f'action: new_review | {review}')
        if review.title in self.results:
            self.results[review.title].update(review)

    def send_results(self):
        self.results = {k:v for k, v in self.results.items() if v.n > 0}
        super().send_results()