import logging

from utils.worker import Worker
from utils.middleware.middleware import Middleware
from utils.serializer.q3ReviewInSerializer import Q3ReviewInSerializer
from utils.serializer.q3PartialSerializer import Q3PartialSerializer

class Query3Worker(Worker):
    def __init__(self, peers, chunk_size, books):
        middleware = Middleware()
        middleware.consume(queue_name='Q3-Reviews', callback=self.recv)
        super().__init__(middleware=middleware,
                         in_serializer=Q3ReviewInSerializer(),
                         out_serializer=Q3PartialSerializer(),
                         peers=peers,
                         chunk_size=chunk_size,)
        self.results = books

    def forward_data(self, data):
        self.middleware.produce(data, 'Q3-Sync')

    def resend(self, data):
        self.middleware.requeue(data, 'Q3-Reviews')

    def work(self, input):
        review = input
        logging.debug(f'action: new_review | review: {review}')
        if review.title in self.results:
            logging.debug(f'action: new_review | result: update | review: {review}')
            self.results[review.title].update(review)

    def send_results(self):
        n = len(self.results)
        self.results = {k:v for k, v in self.results.items() if v.n > 0}
        logging.debug(f'action: filtering_result | result: success | n: {n} >> {len(self.results)}')
        super().send_results()
