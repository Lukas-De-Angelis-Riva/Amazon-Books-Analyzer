import logging
from utils.worker import Worker
from utils.middleware.middleware import Middleware
from utils.serializer.q1InSerializer import Q1InSerializer
from utils.serializer.q1OutSerializer import Q1OutSerializer

class Query1Worker(Worker):
    def __init__(self, peers, chunk_size, matches):
        middleware = Middleware()
        # TODO: no me gusta que se llame a self.recv, parece muy criptico... buscarle la vuelta
        middleware.consume(queue_name='Q1-Books',callback=self.recv)

        super().__init__(middleware=middleware,
                         in_serializer=Q1InSerializer(),
                         out_serializer=Q1OutSerializer(),
                         peers=peers,
                         chunk_size=chunk_size,)
        self.matching_books = []
        self.matches = matches

    def forward_data(self, data):
        self.middleware.publish(data, 'results', 'Q1')

    def resend(self, data):
        self.middleware.requeue(data, 'Q1-Books')
        return super().resend(data)

    def work(self, input):
        book = input
        logging.debug(f'action: new_book | book: {book}')
        if self.matches(book):
            logging.debug(f'action: new_book | result: match | book: {book}')
            self.matching_books.append(book)

    def do_after_work(self):
        if self.matching_books:
            data = self.out_serializer.to_bytes(self.matching_books)
            self.forward_data(data)
        self.matching_books = []