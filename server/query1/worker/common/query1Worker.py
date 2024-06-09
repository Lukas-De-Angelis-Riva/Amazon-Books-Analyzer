import logging
from utils.worker import Worker, WORKER_ID
from utils.middleware.middleware import Middleware
from utils.serializer.q1InSerializer import Q1InSerializer      # type: ignore
from utils.serializer.q1OutSerializer import Q1OutSerializer    # type: ignore
from utils.model.message import Message, MessageType


def IN_QUEUE_NAME(peer_id):
    return f'Q1-Books-{peer_id}'


def OUT_QUEUE_NAME():
    return 'Q1-Sync'


class Query1Worker(Worker):
    def __init__(self, peer_id, peers, chunk_size, matches):
        middleware = Middleware()
        middleware.consume(queue_name=IN_QUEUE_NAME(peer_id), callback=self.recv)

        super().__init__(middleware=middleware,
                         in_serializer=Q1InSerializer(),
                         out_serializer=Q1OutSerializer(),
                         peer_id=peer_id,
                         peers=peers,
                         chunk_size=chunk_size,)
        self.matching_books = []
        self.matches = matches

    def forward_eof(self, eof):
        self.middleware.produce(eof, OUT_QUEUE_NAME())

    def forward_data(self, data):
        self.middleware.produce(data, OUT_QUEUE_NAME())

    def work(self, input, client_id):
        book = input
        logging.debug(f'action: new_book | book: {book}')
        if self.matches(book):
            logging.debug(f'action: new_book | result: match | book: {book}')
            self.matching_books.append(book)

    def do_after_work(self, client_id):
        if self.matching_books:
            data = self.out_serializer.to_bytes(self.matching_books)
            msg = Message(
                client_id=client_id,
                type=MessageType.DATA,
                data=data,
                args={
                    WORKER_ID: self.peer_id,
                }
            )
            self.forward_data(msg.to_bytes())
            self.total_sent += len(self.matching_books)
        self.matching_books = []
