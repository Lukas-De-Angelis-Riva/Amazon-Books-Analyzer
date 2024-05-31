import logging
import io

from utils.protocol import is_eof
from utils.worker2 import Worker2
from utils.middleware.middleware import Middleware
from dto.q3Partial import Q3Partial
from utils.serializer.q3ReviewInSerializer import Q3ReviewInSerializer  # type: ignore
from utils.serializer.q3PartialSerializer import Q3PartialSerializer    # type: ignore
from utils.serializer.q3BookInSerializer import Q3BookInSerializer      # type: ignore
from utils.protocol import get_eof_argument2


class Query3Worker(Worker2):
    def __init__(self, minimum_date, maximum_date, peer_id, peers, chunk_size):
        middleware = Middleware()
        middleware.consume(queue_name='Q3-Reviews',
                           callback=self.recv_raw)
        middleware.subscribe(topic='Q3-EOF', tags=[str(peer_id)], callback=self.recv_eof)

        middleware.subscribe(topic='Q3-Books',
                             tags=[],
                             callback=self.recv_book)

        super().__init__(middleware=middleware,
                         in_serializer=Q3ReviewInSerializer(),
                         out_serializer=Q3PartialSerializer(),
                         peer_id=peer_id,
                         peers=peers,
                         chunk_size=chunk_size,)

        self.maximum_date = maximum_date
        self.minimum_date = minimum_date
        self.book_serializer = Q3BookInSerializer()
        self.results = {}
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

    def recv_book(self, raw, key):
        if is_eof(raw):
            total, worked, sent = get_eof_argument2(raw)
            logging.debug(f'action: recv_book_eof | total_books_sent: {total} | total_books_saved: {self.n_books}')
            if total != self.n_books:
                logging.debug(f'action: recv_book_eof | remaining {total-self.n_books} left')
                return 2
            else:
                logging.debug('action: recv_book_eof | success | all_books_received')
                self.all_books_received = True
                return True
        self.recv_raw_book(raw)
        return True

    #################
    # REVIEW WORKER #
    #################
    def forward_eof(self, eof):
        self.middleware.publish(data=eof, topic='Q3-EOF', tag='SYNC')

    def forward_data(self, data):
        self.middleware.produce(data, 'Q3-Sync')

    def send_to_peer(self, data, peer_id):
        self.middleware.publish(data=data, topic='Q3-EOF', tag=str(peer_id))

    def recv_raw(self, raw, key):
        if not self.all_books_received:
            logging.debug('action: recv_raw | status: not_all_books_received | NACK')
            return 2
        return super().recv_raw(raw, key)

    def work(self, input):
        review = input
        logging.debug(f'action: new_review | review: {review}')
        if review.title in self.results:
            logging.debug(f'action: new_review | result: update | review: {review}')
            self.results[review.title].update(review)

    def do_after_work(self):
        return

    def send_results(self):
        n = len(self.results)
        self.results = {k: v for k, v in self.results.items() if v.n > 0}
        logging.debug(f'action: filtering_result | result: success | n: {n} >> {len(self.results)}')
        super().send_results()
