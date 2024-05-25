import logging

from utils.worker import Worker
from utils.middleware.middleware import Middleware
from dto.q2Partial import Q2Partial
from utils.serializer.q2InSerializer import Q2InSerializer
from utils.serializer.q2PartialSerializer import Q2PartialSerializer

class Query2Worker(Worker):
    def __init__(self, peers, chunk_size):
        middleware = Middleware()
        middleware.consume(queue_name='Q2-Books', callback=self.recv)
        super().__init__(middleware=middleware,
                         in_serializer=Q2InSerializer(),
                         out_serializer=Q2PartialSerializer(),
                         peers=peers,
                         chunk_size=chunk_size,)

    def forward_data(self, data):
        self.middleware.produce(data, 'Q2-Sync')

    def resend(self, data):
        self.middleware.requeue(data, 'Q2-Books')

    def work(self, input):
        book = input
        logging.debug(f'action: new_book | book: {book}')
        for author in book.authors:
            if author not in self.results:
                self.results[author] = Q2Partial(author, [])
            self.results[author].update(book)
            logging.debug(f'action: new_book | result: update | author: {author} | date: {book.publishedDate}')        
