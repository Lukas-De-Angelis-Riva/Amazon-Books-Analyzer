import logging

from utils.worker import Worker
from utils.q2Partial import Q2Partial
from utils.middleware.middlewareQQ import MiddlewareQQ
from utils.serializer.reviewSerializer import ReviewSerializer
from utils.serializer.partialQ3Serializer import PartialQ3Serializer

class Query3Worker(Worker):
    def __init__(self, peers, chunk_size, books):
        middleware = MiddlewareQQ(in_queue_name='Q3-Reviews',
                                  out_queue_name='Q3-Sync')
        super().__init__(middleware=middleware,
                         in_serializer=ReviewSerializer(),
                         out_serializer=PartialQ3Serializer(),
                         peers=peers,
                         chunk_size=chunk_size,)
        self.results = books

    def work(self, input):
        review = input
        logging.info(f'action: new_review | {review}')
        if review.title in self.results:
            self.results[review.title].update(review)
