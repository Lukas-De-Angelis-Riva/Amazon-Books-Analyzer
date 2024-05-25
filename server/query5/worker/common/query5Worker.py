import logging
import io

from utils.protocol import is_eof
from utils.worker import Worker
from utils.middleware.middleware import Middleware
from dto.q5Partial import Q5Partial
from utils.serializer.q5ReviewInSerializer import Q5ReviewInSerializer
from utils.serializer.q5PartialSerializer import Q5PartialSerializer
from utils.serializer.q5BookInSerializer import Q5BookInSerializer

class Query5Worker(Worker):
    def __init__(self, category, peers, chunk_size):
        middleware = Middleware()
        middleware.consume(queue_name='Q5-Reviews', callback=self.recv)
        middleware.subscribe(topic='Q5-Books', tags=[], callback=self.recv_book)

        super().__init__(middleware=middleware,
                         in_serializer=Q5ReviewInSerializer(),
                         out_serializer=Q5PartialSerializer(),
                         peers=peers,
                         chunk_size=chunk_size,)
        self.category = category.lower()
        self.book_serializer = Q5BookInSerializer()
        self.results = {}
        self.all_books_received = False

    ###################
    ### BOOK WORKER ###
    ###################
    def save_book(self, book):
        logging.debug(f'action: new_book | book: {book}')
        if self.category in [c.lower() for c in book.categories]:
            logging.debug(f'action: new_book | result: saving | book: {book}')
            self.results[book.title] = Q5Partial(book.title)

    def recv_raw_book(self, raw):
        reader = io.BytesIO(raw)
        input_chunk = self.book_serializer.from_chunk(reader)
        logging.debug(f'action: new_chunk | chunck_len: {len(input_chunk)}')
        for input in input_chunk:
            self.save_book(input)

    def recv_book(self, raw, key):
        if is_eof(raw):
            self.all_books_received = True
            return True # Because we still want to receive reviews...

        self.recv_raw_book(raw)
        return True

    #####################
    ### REVIEW WORKER ###
    #####################
    def forward_data(self, data):
        self.middleware.produce(data, 'Q5-Sync')

    def resend(self, data):
        self.middleware.requeue(data, 'Q5-Reviews')

    def work(self, input):
        # TODO Check self.all_books_received!!! If not => NACK
        review = input
        logging.debug(f'action: new_review | review: {review}')
        if review.title in self.results:
            logging.debug(f'action: new_review | result: update | review: {review}')
            self.results[review.title].update(review)

    def send_results(self):
        n = len(self.results)
        self.results = {k:v for k, v in self.results.items() if v.n > 0}
        logging.debug(f'action: filtering_result | result: success | n: {n} >> {len(self.results)}')
        super().send_results()