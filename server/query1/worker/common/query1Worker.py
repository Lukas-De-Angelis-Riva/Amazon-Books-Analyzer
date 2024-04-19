import logging
from utils.worker import Worker
from utils.middleware.middlewareQE import MiddlewareQE
from utils.serializer.q1InSerializer import Q1InSerializer
from utils.serializer.q1OutSerializer import Q1OutSerializer

class Query1Worker(Worker):
    def __init__(self, peers, chunk_size, matches):
        middleware = MiddlewareQE(in_queue_name='Q1-Books',
                                  exchange='results',
                                  tag='Q1')
        super().__init__(middleware=middleware,
                         in_serializer=Q1InSerializer(),
                         out_serializer=Q1OutSerializer(),
                         peers=peers,
                         chunk_size=chunk_size,)
        self.matching_books = []
        self.matches = matches

    def work(self, input):
        book = input
        if self.matches(book):
            logging.info(f'action: filtering_books | result: match | value: {book}')
            self.matching_books.append(book)

    def do_after_work(self):
        if self.matching_books:
            data = self.out_serializer.to_bytes(self.matching_books)
            self.middleware.publish(data)
        self.matching_books = []