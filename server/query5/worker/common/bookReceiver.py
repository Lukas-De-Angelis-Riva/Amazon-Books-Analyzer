import logging

from utils.worker import Worker
from dto.q5Partial import Q5Partial
from utils.middleware.middlewareEQ import MiddlewareEQ
from utils.serializer.q5BookInSerializer import Q5BookInSerializer

class BookReceiver(Worker):
    def __init__(self, books, category):
        middleware = MiddlewareEQ(exchange='Q5-Books',
                                  tag='',
                                  out_queue_name=None)
        super().__init__(middleware=middleware,
                         in_serializer=Q5BookInSerializer(),
                         out_serializer=None,
                         peers=0,
                         chunk_size=0)
        self.books = books
        self.category = category.lower()

    def work(self, input):
        book = input

        logging.debug(f'action: new_book | book: {book}')
        if self.category in [c.lower() for c in book.categories]:
            logging.debug(f'action: new_book | result: saving | book: {book}')
            self.books[book.title] = Q5Partial(book.title)

    def recv_eof(self, eof):
        return
