import logging

from utils.worker import Worker
from utils.middleware.middleware import Middleware
from dto.q2Partial import Q2Partial
from utils.serializer.q2InSerializer import Q2InSerializer              # type: ignore
from utils.serializer.q2PartialSerializer import Q2PartialSerializer    # type: ignore


class Query2Worker(Worker):
    def __init__(self, peer_id, peers, chunk_size):
        middleware = Middleware()
        middleware.consume(queue_name='Q2-Books', callback=self.recv_raw)
        middleware.subscribe(topic='Q2-EOF', tags=[str(peer_id)], callback=self.recv_eof)
        super().__init__(middleware=middleware,
                         in_serializer=Q2InSerializer(),
                         out_serializer=Q2PartialSerializer(),
                         peer_id=peer_id,
                         peers=peers,
                         chunk_size=chunk_size,)

    def forward_eof(self, eof):
        self.middleware.publish(data=eof, topic='Q2-EOF', tag='SYNC')

    def forward_data(self, data):
        self.middleware.produce(data, 'Q2-Sync')

    def send_to_peer(self, data, peer_id):
        self.middleware.publish(data=data, topic='Q2-EOF', tag=str(peer_id))

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
