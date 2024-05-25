import logging
import io
from utils.worker import Worker
from utils.middleware.middleware import Middleware
from utils.serializer.q1InSerializer import Q1InSerializer      # type: ignore
from utils.serializer.q1OutSerializer import Q1OutSerializer    # type: ignore
from utils.protocol import make_eof, get_eof_argument


class Query1Worker(Worker):
    def __init__(self, peer_id, peers, chunk_size, matches):
        middleware = Middleware()
        # TODO: no me gusta que se llame a self.recv, parece muy criptico... buscarle la vuelta
        middleware.consume(queue_name='Q1-Books', callback=self.recv)
        middleware.subscribe(topic='Q1-EOF', tags=[str(peer_id)], callback=self.recv_eof)

        super().__init__(middleware=middleware,
                         in_serializer=Q1InSerializer(),
                         out_serializer=Q1OutSerializer(),
                         peers=peers,
                         chunk_size=chunk_size,)
        self.matching_books = []
        self.matches = matches
        self.peer_id = peer_id
        self.worked_books = 0
        self.remaining_books = -1
        self.received_eof = False

    def recv(self, raw, key):
        if self.peer_id != self.peers and self.received_eof:
            logging.info('action: recv | status: success | NACK')
            return 2

        reader = io.BytesIO(raw)
        input_chunk = self.in_serializer.from_chunk(reader)
        logging.info(f'action: recv | status: new_chunk | len(chunk): {len(input_chunk)}')

        for input in input_chunk:
            self.work(input)
        self.do_after_work()
        self.worked_books += len(input_chunk)
        logging.info(f'action: recv | status: success | worked_books: {self.worked_books}')

        if self.remaining_books >= 0 and self.remaining_books == self.worked_books:
            logging.info('action: recv | status: success | forwarding_eof...')
            # SEND RESULTS IF NECESARY
            eof = make_eof()
            self.forward_data(eof)
        return True

    def recv_eof(self, eof, key):
        remaining_books = get_eof_argument(eof)
        logging.info(f'action: recv_eof | status: in_progress | remaining_books: {remaining_books}')
        if self.peer_id == self.peers:
            if remaining_books == self.worked_books:
                eof = make_eof()
                self.forward_data(eof)
                logging.info('action: recv_eof | status: success | forwarding_eof')
            else:
                logging.info('action: recv_eof | status: success | waiting...')
                self.remaining_books = remaining_books
        else:
            new_remaining_books = remaining_books - self.worked_books
            eof = make_eof(i=new_remaining_books)
            # aca enviar a synchronizer, pero como query1 no tiene no hace falta.
            self.middleware.publish(data=eof, topic='Q1-EOF', tag=str(self.peer_id+1))
            logging.info(f'action: recv_eof | status: success | new_eof: to{self.peer_id+1}')

        self.received_eof = True
        return True

    def forward_data(self, data):
        self.middleware.publish(data, 'results', 'Q1')

    def resend(self, data):
        self.middleware.requeue(data, 'Q1-Books')

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
