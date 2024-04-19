import logging

from utils.worker import Worker
from utils.q2Partial import Q2Partial
from utils.middleware.middlewareQQ import MiddlewareQQ
from utils.serializer.bookSerializer import BookSerializer
from utils.serializer.partialQ2Serializer import PartialQ2Serializer

class Query2Worker(Worker):
    def __init__(self, peers, chunk_size):
        middleware = MiddlewareQQ(in_queue_name='Q2-Books',
                                  out_queue_name='Q2-Sync')
        super().__init__(middleware=middleware,
                         in_serializer=BookSerializer(),
                         out_serializer=PartialQ2Serializer(),
                         peers=peers,
                         chunk_size=chunk_size,)

    def work(self, input):
        book = input
        logging.info(f'action: new_book | {book}')
        for author in book.authors:
            if author not in self.results:
                self.results[author] = Q2Partial(author, [])
            self.results[author].update(book)
            logging.info(f'action: updating partial | author: {author} | date: {book.publishedDate}')