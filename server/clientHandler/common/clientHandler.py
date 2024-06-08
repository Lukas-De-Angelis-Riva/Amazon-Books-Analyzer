import uuid
import socket
import signal
import logging

from model.book import Book
from common.sharder import shard
from common.clientHandlerMiddleware import ClientHandlerMiddleware
from utils.serializer.bookSerializer import BookSerializer
from utils.serializer.reviewSerializer import ReviewSerializer
from utils.serializer.q1InSerializer import Q1InSerializer
from utils.serializer.q2InSerializer import Q2InSerializer
from utils.serializer.q3BookInSerializer import Q3BookInSerializer
from utils.serializer.q3ReviewInSerializer import Q3ReviewInSerializer
from utils.serializer.q5BookInSerializer import Q5BookInSerializer
from utils.serializer.q5ReviewInSerializer import Q5ReviewInSerializer
from utils.protocolHandler import ProtocolHandler
from utils.TCPhandler import SocketBroken
from utils.protocol import make_eof2
from utils.model.message import Message, MessageType

TOTAL = "total"
QUERY1_ID = 'Q1'
QUERY2_ID = 'Q2'
QUERY3_ID = 'Q3'
QUERY5_ID = 'Q5'


def OUT_BOOKS_QUEUE(query_id, worker_id):
    return f'{query_id}-Books-{worker_id}'


def OUT_REVIEWS_QUEUE(query_id, worker_id):
    return f'{query_id}-Reviews-{worker_id}'


class ClientHandler:
    def __init__(self, config_params):
        signal.signal(signal.SIGTERM, self.__handle_signal)

        # Initialize server socket
        self.config_params = config_params
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', config_params['port']))
        self._server_socket.listen(1)
        self._server_on = True

        self.middleware = ClientHandlerMiddleware()

        # client book and review serializers
        self.book_serializer = BookSerializer()
        self.review_serializer = ReviewSerializer()

        # system book and review serializers
        self.book_serializers = {
            QUERY1_ID: Q1InSerializer(),
            QUERY2_ID: Q2InSerializer(),
            QUERY3_ID: Q3BookInSerializer(),
            QUERY5_ID: Q5BookInSerializer(),
        }
        self.review_serializers = {
            QUERY3_ID: Q3ReviewInSerializer(),
            QUERY5_ID: Q5ReviewInSerializer(),
        }

        # book and review counters
        self.total_books = {
            QUERY1_ID: {i: 0 for i in range(1, 3)},     # config_params[WORKERS_Q1]
            QUERY2_ID: {i: 0 for i in range(1, 3)},     # config_params[WORKERS_Q2]
            QUERY3_ID: {i: 0 for i in range(1, 3)},     # config_params[WORKERS_Q3]
            QUERY5_ID: {i: 0 for i in range(1, 6)},     # config_params[WORKERS_Q5]
        }
        self.total_reviews = {
            QUERY3_ID: {i: 0 for i in range(1, 3)},     # config_params[WORKERS_Q3]
            QUERY5_ID: {i: 0 for i in range(1, 6)},     # config_params[WORKERS_Q5]
        }

    def run(self):
        logging.info('action: run server | result: success')
        while self._server_on:
            client_sock = self.__accept_new_connection()
            if client_sock:
                self.__handle_client_connection(client_sock)

        self._server_socket.close()
        logging.debug('action: release_socket | result: success')
        self.middleware.stop()
        logging.debug('action: release_rabbitmq_conn | result: success')
        logging.info('action: stop_server | result: success')

    def __handle_client_connection(self, client_sock):
        client_id = uuid.uuid4()
        try:
            protocolHandler = ProtocolHandler(client_sock)
            keep_reading = True
            while keep_reading:
                t, value = protocolHandler.read()

                if protocolHandler.is_review(t):
                    keep_reading = self.__handle_reviews(value, client_id)
                elif protocolHandler.is_book(t):
                    keep_reading = self.__handle_books(value, client_id)
                elif protocolHandler.is_book_eof(t):
                    keep_reading = self.__handle_book_eof(client_id)
                elif protocolHandler.is_review_eof(t):
                    keep_reading = self.__handle_review_eof(client_id)

                protocolHandler.ack()

        except (SocketBroken, OSError) as e:
            logging.error(f'action: receive_message | result: fail | error: {str(e)}')
        finally:
            if client_sock:
                logging.debug('action: release_client_socket | result: success')
                client_sock.close()
                logging.debug('action: finishing | result: success')

    def __send_book_eof(self, query_id, client_id):
        for worker_i in self.total_books[query_id]:
            eof = Message(
                client_id=client_id,
                type=MessageType.EOF,
                data=b'',
                args={
                    TOTAL: self.total_books[query_id][worker_i]
                }
            )
            self.middleware.produce(
                eof.to_bytes(),
                out_queue_name=OUT_BOOKS_QUEUE(query_id, worker_i)
            )

    def __handle_book_eof(self, client_id):
        self.__send_book_eof(QUERY1_ID, client_id)
        self.__send_book_eof(QUERY2_ID, client_id)
        self.__send_book_eof(QUERY3_ID, client_id)
        self.__send_book_eof(QUERY5_ID, client_id)
        logging.debug('action: send_books | value: EOF | result: success')
        return True

    def __group_by_key(self, chunk, n, get_key):
        grouped = [[] for _ in range(n)]
        for value in chunk:
            grouped[shard(get_key(value), n)].append(value)
        return grouped

    def __explode_by_authors(self, books):
        new_books = []
        for book in books:
            for author in book.authors:
                aux_book = Book(
                    title=book.title,
                    authors=[author],
                    publisher=book.publisher,
                    publishedDate=book.publishedDate,
                    categories=book.categories,
                )
                new_books.append(aux_book)
        return new_books

    def __distribute_books(self, sharded_chunks: list, query_id: str, client_id):
        n_workers = len(sharded_chunks)
        for worker_i in range(1, n_workers+1):
            i = worker_i - 1
            if not sharded_chunks[i]:
                continue
            self.total_books[query_id][worker_i] += len(sharded_chunks[i])
            data_wi = self.book_serializers[query_id].to_bytes(sharded_chunks[i])
            msg = Message(
                client_id=client_id,
                type=MessageType.DATA,
                data=data_wi,
            )
            self.middleware.produce(
                msg.to_bytes(),
                out_queue_name=OUT_BOOKS_QUEUE(query_id, worker_i)
            )

    def __handle_books(self, value, client_id):
        # Query 1:
        q1_n_workers = 2    # config_params[WORKERS_Q1]
        value_grouped_by_title = self.__group_by_key(value, q1_n_workers, lambda b: b.title)
        self.__distribute_books(value_grouped_by_title, QUERY1_ID, client_id)

        # Query 2:
        q2_n_workers = 2    # config_params[WORKERS_Q2]
        exploded = self.__explode_by_authors(value)
        value_grouped_by_author = self.__group_by_key(exploded, q2_n_workers, lambda b: b.authors[0])
        self.__distribute_books(value_grouped_by_author, QUERY2_ID, client_id)

        # Query 3/4:
        q3_n_workers = 2    # config_params[WORKERS_Q3]
        value_grouped_by_title = self.__group_by_key(value, q3_n_workers, lambda b: b.title)
        self.__distribute_books(value_grouped_by_title, QUERY3_ID, client_id)

        # Query 5:
        q5_n_workers = 5    # config_params[WORKERS_Q5]
        value_grouped_by_title = self.__group_by_key(value, q5_n_workers, lambda b: b.title)
        self.__distribute_books(value_grouped_by_title, QUERY5_ID, client_id)

        logging.debug(f'action: send_books | N: {len(value)} | result: success')
        return True

    def __send_review_eof(self, query_id, client_id):
        for worker_i in self.total_reviews[query_id]:
            eof = Message(
                client_id=client_id,
                type=MessageType.EOF,
                data=b'',
                args={
                    TOTAL: self.total_reviews[query_id][worker_i]
                }
            )
            self.middleware.produce(
                eof.to_bytes(),
                out_queue_name=OUT_REVIEWS_QUEUE(query_id, worker_i)
            )

    def __handle_review_eof(self, client_id):
        self.__send_review_eof(QUERY3_ID, client_id)
        self.__send_review_eof(QUERY5_ID, client_id)
        logging.debug('action: review_eof | value: EOF | result: success')
        return True

    def __distribute_reviews(self, sharded_chunks: list, query_id: str, client_id):
        n_workers = len(sharded_chunks)
        for worker_i in range(1, n_workers+1):
            i = worker_i - 1
            if not sharded_chunks[i]:
                continue
            self.total_reviews[query_id][worker_i] += len(sharded_chunks[i])
            data_wi = self.review_serializers[query_id].to_bytes(sharded_chunks[i])
            msg = Message(
                client_id=client_id,
                type=MessageType.DATA,
                data=data_wi,
            )
            self.middleware.produce(
                msg.to_bytes(),
                out_queue_name=OUT_REVIEWS_QUEUE(query_id, worker_i)
            )

    def __handle_reviews(self, reviews, client_id):
        # Query 3/4:
        q3_n_workers = 2    # config_params[WORKERS_Q3]
        reviews_grouped_by_title = self.__group_by_key(reviews, q3_n_workers, lambda r: r.title)
        self.__distribute_reviews(reviews_grouped_by_title, QUERY3_ID, client_id)

        # Query 5:
        q5_n_workers = 5    # config_params[WORKERS_Q5]
        reviews_grouped_by_author = self.__group_by_key(reviews, q5_n_workers, lambda r: r.title)
        self.__distribute_reviews(reviews_grouped_by_author, QUERY5_ID, client_id)

        logging.debug(f'action: send_reviews | N: {len(reviews)} | result: success')
        return True

    def __accept_new_connection(self):
        try:
            logging.debug('action: accept_connections | result: in_progress')
            c, addr = self._server_socket.accept()
            logging.debug(f'action: accept_connections | result: success | ip: {addr[0]}')
            logging.info(f'action: new client | ip: {addr[0]}')
            return c
        except OSError as e:
            if self._server_on:
                logging.error(f'action: accept_connections | result: fail | error: {str(e)}')
            else:
                logging.debug('action: stop_accept_connections | result: success')
            return

    def __handle_signal(self, signum, frame):
        logging.info(f'action: stop_server | result: in_progress | signal {signum}')
        self._server_on = False
        self._server_socket.shutdown(socket.SHUT_RDWR)
        logging.debug('action: shutdown_socket | result: success')
