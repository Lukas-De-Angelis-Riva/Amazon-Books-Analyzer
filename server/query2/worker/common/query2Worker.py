import logging

from utils.worker import Worker
from utils.middleware.middlewareQQ import MiddlewareQQ
from dto.q2Partial import Q2Partial
from utils.serializer.q2InSerializer import Q2InSerializer
from utils.serializer.q2PartialSerializer import Q2PartialSerializer

class Query2Worker(Worker):
    def __init__(self, peers, chunk_size):
        middleware = MiddlewareQQ(in_queue_name='Q2-Books',
                                  out_queue_name='Q2-Sync')
        super().__init__(middleware=middleware,
                         in_serializer=Q2InSerializer(),
                         out_serializer=Q2PartialSerializer(),
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