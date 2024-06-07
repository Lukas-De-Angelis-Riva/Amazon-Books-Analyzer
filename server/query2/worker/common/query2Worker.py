import logging

from utils.worker import Worker
from utils.middleware.middleware import Middleware
from dto.q2Partial import Q2Partial
from utils.serializer.q2InSerializer import Q2InSerializer              # type: ignore
from utils.serializer.q2PartialSerializer import Q2PartialSerializer    # type: ignore


def in_queue_name(peer_id):
    return f'Q2-Books-{peer_id}'


def out_queue_name():
    return 'Q2-Sync'


class Query2Worker(Worker):
    def __init__(self, peer_id, peers, chunk_size):
        middleware = Middleware()
        middleware.consume(queue_name=in_queue_name(peer_id), callback=self.recv)

        super().__init__(middleware=middleware,
                         in_serializer=Q2InSerializer(),
                         out_serializer=Q2PartialSerializer(),
                         peer_id=peer_id,
                         peers=peers,
                         chunk_size=chunk_size,)

    def forward_eof(self, eof):
        self.middleware.produce(eof, out_queue_name())

    def forward_data(self, data):
        self.middleware.produce(data, out_queue_name())

    def work(self, input):
        book = input
        logging.debug(f'action: new_book | book: {book}')
        for author in book.authors:
            if author not in self.results:
                self.results[author] = Q2Partial(author, [])
            self.results[author].update(book)
            logging.debug(f'action: new_book | result: update | author: {author} | date: {book.publishedDate}')

    def do_after_work(self):
        return
