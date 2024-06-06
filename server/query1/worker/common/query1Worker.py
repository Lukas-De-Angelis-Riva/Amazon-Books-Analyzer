import logging
from utils.worker import Worker
from utils.middleware.middleware import Middleware
from utils.serializer.q1InSerializer import Q1InSerializer      # type: ignore
from utils.serializer.q1OutSerializer import Q1OutSerializer    # type: ignore


class Query1Worker(Worker):
    def __init__(self, peer_id, peers, chunk_size, matches):
        middleware = Middleware()
        middleware.consume(queue_name='Q1-Books', callback=self.recv_raw)
        middleware.subscribe(topic='Q1-RING', tags=[str(peer_id)], callback=self.recv_token)

        super().__init__(middleware=middleware,
                         in_serializer=Q1InSerializer(),
                         out_serializer=Q1OutSerializer(),
                         peer_id=peer_id,
                         peers=peers,
                         chunk_size=chunk_size,)
        self.matching_books = []
        self.matches = matches

    def forward_eof(self, eof):
        self.middleware.publish(eof, 'results', 'Q1')

    def forward_data(self, data):
        self.middleware.publish(data, 'results', 'Q1')

    def send_to_peer(self, data, peer_id):
        self.middleware.publish(data=data, topic='Q1-EOF', tag=str(peer_id))

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
