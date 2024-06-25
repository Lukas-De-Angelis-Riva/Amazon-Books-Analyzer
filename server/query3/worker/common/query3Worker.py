import logging
import io

from utils.worker import Worker, TOTAL
from utils.middleware.middleware import Middleware, ACK, NACK
from dto.q3Partial import Q3Partial
from utils.serializer.q3ReviewInSerializer import Q3ReviewInSerializer  # type: ignore
from utils.serializer.q3PartialSerializer import Q3PartialSerializer    # type: ignore
from utils.serializer.q3BookInSerializer import Q3BookInSerializer      # type: ignore
from utils.model.message import Message, MessageType

# TEST PURPOSES
from utils.model.virus import virus


def IN_BOOKS_QUEUE_NAME(peer_id):
    return f'Q3-Books-{peer_id}'


def IN_REVIEWS_QUEUE_NAME(peer_id):
    return f'Q3-Reviews-{peer_id}'


def OUT_QUEUE_NAME():
    return 'Q3-Sync'


N_BOOKS = "N_BOOKS"
ALL_BOOKS_RECEIVED = "ALL_BOOKS_RECEIVED"


class Query3Worker(Worker):
    def __init__(self, min_amount_reviews, minimum_date, maximum_date, peer_id, peers, chunk_size, test_middleware=None):
        middleware = test_middleware if test_middleware else Middleware()
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

        self.recovery()

    def adapt_tracker(self):
        if N_BOOKS not in self.tracker.meta_data:
            self.tracker.log_manager.integers.append(N_BOOKS)
            self.tracker.meta_data[N_BOOKS] = 0
        if ALL_BOOKS_RECEIVED not in self.tracker.meta_data:
            self.tracker.log_manager.booleans.append(ALL_BOOKS_RECEIVED)
            self.tracker.meta_data[ALL_BOOKS_RECEIVED] = False

        self.tracker.parser = Q3Partial.decode

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

    def recv_raw_book(self, raw, chunk_id):
        reader = io.BytesIO(raw)
        input_chunk = self.book_serializer.from_chunk(reader)
        logging.debug(f'action: new_chunk | chunck_len: {len(input_chunk)}')
        for input in input_chunk:
            self.save_book(input)

        virus.infect()
        self.tracker.persist(chunk_id, flush_data=True,
                             N_BOOKS=self.tracker.meta_data[N_BOOKS]+len(input_chunk))
        virus.infect()

    def recv_book(self, raw_msg, key):
        msg = Message.from_bytes(raw_msg)
        if msg.client_id in self.worked_clients:
            return ACK

        self.context_switch(msg.client_id)

        if msg.ID in self.tracker.worked_chunks:
            return ACK

        if msg.type == MessageType.EOF:
            if msg.args[TOTAL] != self.tracker.meta_data[N_BOOKS]:
                virus.infect()
                diff = msg.args[TOTAL]-self.tracker.meta_data[N_BOOKS]
                logging.debug(f'action: recv_book_eof | remaining: {diff} left')
                return NACK
            else:
                virus.infect()
                self.tracker.persist(msg.ID, ALL_BOOKS_RECEIVED=True)
                virus.infect()
                logging.debug('action: recv_book_eof | success | all_books_received')
                return ACK
        self.recv_raw_book(msg.data, msg.ID)
        virus.infect()
        return ACK

    #################
    # REVIEW WORKER #
    #################
    def recv(self, raw_msg, key):
        msg = Message.from_bytes(raw_msg)
        if msg.client_id in self.worked_clients:
            return ACK
        self.context_switch(msg.client_id)
        if not self.tracker.meta_data[ALL_BOOKS_RECEIVED]:
            logging.debug('action: recv_raw | status: not_all_books_received | NACK')
            virus.infect()
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
            old = self.tracker.data[review.title].copy()
            self.tracker.data[review.title].update(review)
            new = self.tracker.data[review.title].copy()

            self.tracker.log_manager.hold_change(review.title, old, new)

    def do_after_work(self, chunk_id):
        return

    def terminator(self):
        results = [v for v in self.tracker.data.values() if v.n >= self.min_amount_reviews]
        if results:
            logging.debug(f'action: filtering_result | result: success | n: {len(self.tracker.data)} >> {len(results)}')
            virus.infect()
            self.send_results(results)
        virus.infect()
        self.send_eof(len(results))
