import logging
import io

from utils.worker import Worker, ClientTracker, TOTAL
from utils.middleware.middleware import Middleware, ACK, NACK
from dto.q3Partial import Q3Partial
from utils.serializer.q3ReviewInSerializer import Q3ReviewInSerializer  # type: ignore
from utils.serializer.q3PartialSerializer import Q3PartialSerializer    # type: ignore
from utils.serializer.q3BookInSerializer import Q3BookInSerializer      # type: ignore
from utils.model.message import Message, MessageType


def IN_BOOKS_QUEUE_NAME(peer_id):
    return f'Q3-Books-{peer_id}'


def IN_REVIEWS_QUEUE_NAME(peer_id):
    return f'Q3-Reviews-{peer_id}'


def OUT_QUEUE_NAME():
    return 'Q3-Sync'


class BookTracker(ClientTracker):
    def __init__(self, client_id):
        super().__init__(client_id)
        self.n_books = 0
        self.all_books_received = False


class Query3Worker(Worker):
    def __init__(self, min_amount_reviews, minimum_date, maximum_date, peer_id, peers, chunk_size):
        middleware = Middleware()
        middleware.consume(queue_name=IN_BOOKS_QUEUE_NAME(peer_id), callback=self.recv_book)
        middleware.consume(queue_name=IN_REVIEWS_QUEUE_NAME(peer_id), callback=self.recv)

        super().__init__(middleware=middleware,
                         in_serializer=Q3ReviewInSerializer(),
                         out_serializer=Q3PartialSerializer(),
                         peer_id=peer_id,
                         peers=peers,
                         chunk_size=chunk_size,)

        self.min_amount_reviews = min_amount_reviews
        self.maximum_date = maximum_date
        self.minimum_date = minimum_date
        self.book_serializer = Q3BookInSerializer()

    ###############
    # BOOK WORKER #
    ###############
    def matches_criteria(self, book):
        published_date = int(book.publishedDate)
        return published_date <= self.maximum_date \
            and published_date >= self.minimum_date

    def save_book(self, book):
        logging.debug(f'action: new_book | book: {book}')
        if self.matches_criteria(book):
            logging.debug(f'action: new_book | result: saving | book: {book}')
            self.tracker.data[book.title] = Q3Partial(book.title, book.authors)

    def recv_raw_book(self, raw):
        reader = io.BytesIO(raw)
        input_chunk = self.book_serializer.from_chunk(reader)
        logging.debug(f'action: new_chunk | chunck_len: {len(input_chunk)}')
        for input in input_chunk:
            self.save_book(input)
        self.tracker.n_books += len(input_chunk)

    def recv_book(self, raw_msg, key):
        msg = Message.from_bytes(raw_msg)
        if msg.client_id not in self.clients:
            self.clients[msg.client_id] = BookTracker(msg.client_id)
        self.tracker = self.clients[msg.client_id]

        if msg.type == MessageType.EOF:
            if msg.args[TOTAL] != self.tracker.n_books:
                logging.debug(f'action: recv_book_eof | remaining {msg.args[TOTAL]-self.tracker.n_books} left')
                return NACK
            else:
                logging.debug('action: recv_book_eof | success | all_books_received')
                self.tracker.all_books_received = True
                return ACK
        self.recv_raw_book(msg.data)
        return ACK

    #################
    # REVIEW WORKER #
    #################
    def recv(self, raw_msg, key):
        msg = Message.from_bytes(raw_msg)
        if msg.client_id not in self.clients:
            self.clients[msg.client_id] = BookTracker(msg.client_id)
        self.tracker = self.clients[msg.client_id]
        if not self.tracker.all_books_received:
            logging.debug('action: recv_raw | status: not_all_books_received | NACK')
            return NACK
        return super().recv(raw_msg, key)

    def forward_eof(self, eof):
        self.middleware.produce(eof, OUT_QUEUE_NAME())

    def forward_data(self, data):
        self.middleware.produce(data, OUT_QUEUE_NAME())

    def work(self, input):
        review = input
        logging.debug(f'action: new_review | review: {review}')
        if review.title in self.tracker.data:
            logging.debug(f'action: new_review | result: update | review: {review}')
            self.tracker.data[review.title].update(review)

    def do_after_work(self, chunk_id):
        return

    def send_results(self):
        n = len(self.tracker.data)
        self.tracker.results = {k: v for k, v in self.tracker.data.items() if v.n >= self.min_amount_reviews}
        logging.debug(f'action: filtering_result | result: success | n: {n} >> {len(self.tracker.results)}')
        super().send_results()
