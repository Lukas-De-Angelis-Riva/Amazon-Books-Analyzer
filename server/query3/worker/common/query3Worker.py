import logging
import io

from utils.worker import Worker, TOTAL
from utils.middleware.middleware import Middleware
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
        self.n_books = 0
        self.all_books_received = False

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
            self.results[book.title] = Q3Partial(book.title, book.authors)

    def recv_raw_book(self, raw):
        reader = io.BytesIO(raw)
        input_chunk = self.book_serializer.from_chunk(reader)
        logging.debug(f'action: new_chunk | chunck_len: {len(input_chunk)}')
        for input in input_chunk:
            self.save_book(input)
        self.n_books += len(input_chunk)

    def recv_book(self, raw_msg, key):
        msg = Message.from_bytes(raw_msg)
        if msg.type == MessageType.EOF:
            if msg.args[TOTAL] != self.n_books:
                logging.debug(f'action: recv_book_eof | remaining {msg.args[TOTAL]-self.n_books} left')
                return 2
            else:
                logging.debug('action: recv_book_eof | success | all_books_received')
                self.all_books_received = True
                return True
        self.recv_raw_book(msg.data)
        return True

    #################
    # REVIEW WORKER #
    #################
    def recv(self, raw_msg, key):
        if not self.all_books_received:
            logging.debug('action: recv_raw | status: not_all_books_received | NACK')
            return 2
        return super().recv(raw_msg, key)

    def forward_eof(self, eof):
        self.middleware.produce(eof, OUT_QUEUE_NAME())

    def forward_data(self, data):
        self.middleware.produce(data, OUT_QUEUE_NAME())

    def work(self, input, client_id):
        review = input
        logging.debug(f'action: new_review | review: {review}')
        if review.title in self.results:
            logging.debug(f'action: new_review | result: update | review: {review}')
            self.results[review.title].update(review)

    def do_after_work(self, client_id):
        return

    def send_results(self, client_id):
        n = len(self.results)
        self.results = {k: v for k, v in self.results.items() if v.n > self.min_amount_reviews}
        logging.debug(f'action: filtering_result | result: success | n: {n} >> {len(self.results)}')
        super().send_results(client_id)
