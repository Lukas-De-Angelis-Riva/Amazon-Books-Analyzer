import logging
from utils.worker import Worker
from model.book import Book
from utils.middleware.middlewareQE import MiddlewareQE
from utils.serializer.bookSerializer import BookSerializer
from utils.serializer.resultQ1Serializer import ResultQ1Serializer

class Query1Worker(Worker):
    def __init__(self, peers):
        middleware = MiddlewareQE(in_queue_name='Q1-Books',
                                  exchange='results',
                                  tag='Q1')
        super().__init__(middleware=middleware,
                         in_serializer=BookSerializer(),
                         out_serializer=ResultQ1Serializer(),
                         peers=peers,
                         chunk_size=1,)
        self.matching_books = []

    def matches(self, book):
        logging.info(f'action: TEST | date:{book.publishedDate}, categories:{book.categories}, title:{book.title}')
        return int(book.publishedDate) >= 2000 and \
            int(book.publishedDate) <= 2023 and \
            'computers' in [c.lower() for c in book.categories] and \
            'distributed' in book.title.lower()

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