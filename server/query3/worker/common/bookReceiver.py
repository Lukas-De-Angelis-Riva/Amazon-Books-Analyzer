import logging

from utils.worker import Worker
from dto.q3Partial import Q3Partial
from utils.middleware.middlewareEQ import MiddlewareEQ
from utils.serializer.q3BookInSerializer import Q3BookInSerializer

class BookReceiver(Worker):
    def __init__(self, books, minimun_date, maximun_date):
        middleware = MiddlewareEQ(exchange='Q3-Books',
                                  tag='',
                                  out_queue_name=None)
        super().__init__(middleware=middleware,
                         in_serializer=Q3BookInSerializer(),
                         out_serializer=None,
                         peers=0,
                         chunk_size=0)
        self.books = books
        self.minimun_date = minimun_date
        self.maximun_date = maximun_date

    def work(self, input):
        book = input
        published_date = int(book.publishedDate)

        logging.debug(f'action: new_book | book: {book}')
        if published_date <= self.maximun_date and published_date >= self.minimun_date:
            logging.debug(f'action: new_book | result: saving | book: {book}')
            self.books[book.title] = Q3Partial(book.title, book.authors)

    def recv_eof(self, eof):
        return
