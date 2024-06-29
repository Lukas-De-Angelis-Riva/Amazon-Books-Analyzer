import logging

from utils.worker import Worker
from utils.middleware.middleware import Middleware
from dto.q2Partial import Q2Partial
from utils.serializer.q2InSerializer import Q2InSerializer              # type: ignore
from utils.serializer.q2OutSerializer import Q2OutSerializer            # type: ignore


def in_queue_name(peer_id):
    return f'Q2-Books-{peer_id}'


def out_queue_name():
    return 'Q2-Sync'


class Query2Worker(Worker):
    def __init__(self, peer_id, peers, chunk_size, min_decades, test_middleware=None):
        middleware = test_middleware if test_middleware else Middleware()
        middleware.consume(queue_name=in_queue_name(peer_id), callback=self.recv)

        super().__init__(middleware=middleware,
                         in_serializer=Q2InSerializer(),
                         out_serializer=Q2OutSerializer(),
                         peer_id=peer_id,
                         peers=peers,
                         chunk_size=chunk_size,)
        self.min_decades = min_decades

        self.recovery()

    def adapt_tracker(self):
        self.tracker.parser = Q2Partial.decode

    def forward_eof(self, eof):
        self.middleware.produce(eof, out_queue_name())

    def forward_data(self, data):
        self.middleware.produce(data, out_queue_name())

    def work(self, input):
        book = input
        author = book.authors[0]
        logging.debug(f'action: new_book | book: {book}')
        if author not in self.tracker.data:
            self.tracker.data[author] = Q2Partial(author, [])

        old = self.tracker.data[author].copy()
        self.tracker.data[author].update(book)
        new = self.tracker.data[author].copy()

        if len(old.decades) != len(new.decades):
            self.tracker.log_manager.hold_change(author, old, new)
            logging.debug(f'action: new_book | result: update | author: {author} | date: {book.publishedDate}')

    def do_after_work(self, chunk_id):
        return

    def filter_results(self):
        return {k: v.author for k, v in self.tracker.data.items() if len(v.decades) >= self.min_decades}

    def terminator(self):
        results = self.filter_results()
        if results:
            logging.debug(f'action: filtering_result | result: success | n: {len(self.tracker.data)} >> {len(results)}')
            self.send_results(results)
        self.send_eof(len(results))
