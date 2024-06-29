from model.book import Book
from common.sharder import shard
from utils.middleware.middleware import Middleware
from utils.serializer.q1InSerializer import Q1InSerializer              # type: ignore
from utils.serializer.q2InSerializer import Q2InSerializer              # type: ignore
from utils.serializer.q3BookInSerializer import Q3BookInSerializer      # type: ignore
from utils.serializer.q3ReviewInSerializer import Q3ReviewInSerializer  # type: ignore
from utils.serializer.q5BookInSerializer import Q5BookInSerializer      # type: ignore
from utils.serializer.q5ReviewInSerializer import Q5ReviewInSerializer  # type: ignore
from utils.persistentMap import PersistentMap
from utils.model.message import Message, MessageType

import os

TOTAL = "total"
QUERY1_ID = 'Q1'
QUERY2_ID = 'Q2'
QUERY3_ID = 'Q3'
QUERY5_ID = 'Q5'
LAST_CHUNK = 'LAST_CHUNK'


def OUT_BOOKS_QUEUE(query_id, worker_id):
    return f'{query_id}-Books-{worker_id}'


def OUT_REVIEWS_QUEUE(query_id, worker_id):
    return f'{query_id}-Reviews-{worker_id}'


def group_by_key(chunk, n, get_key):
    grouped = [[] for _ in range(n)]
    for value in chunk:
        grouped[shard(get_key(value), n)].append(value)
    return grouped


def explode_by_authors(books):
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


class QueryManager:
    def __init__(self, client_id, workers_by_query):
        self.middleware = Middleware()
        self.client_id = client_id
        self.workers_by_query = workers_by_query

        if str(client_id) and not os.path.exists(str(client_id)):
            os.mkdir(str(client_id))

        self.total_books = PersistentMap(f'{client_id}/books_worked', {
            QUERY1_ID: {str(i): 0 for i in range(1, workers_by_query[QUERY1_ID]+1)},
            QUERY2_ID: {str(i): 0 for i in range(1, workers_by_query[QUERY2_ID]+1)},
            QUERY3_ID: {str(i): 0 for i in range(1, workers_by_query[QUERY3_ID]+1)},
            QUERY5_ID: {str(i): 0 for i in range(1, workers_by_query[QUERY5_ID]+1)},
            LAST_CHUNK: "",
        })

        self.total_books.load(lambda k, v: v)

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
        self.total_reviews = PersistentMap(f'{client_id}/reviews_worked', {
            QUERY3_ID: {str(i): 0 for i in range(1, workers_by_query[QUERY3_ID]+1)},
            QUERY5_ID: {str(i): 0 for i in range(1, workers_by_query[QUERY5_ID]+1)},
            LAST_CHUNK: "",
        })

        self.total_reviews.load(lambda k, v: v)

    def __send_book_eof(self, query_id):
        for worker_i in self.total_books[query_id]:
            eof = Message(
                client_id=self.client_id,
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

    def terminate_books(self):
        self.__send_book_eof(QUERY1_ID)
        self.__send_book_eof(QUERY2_ID)
        self.__send_book_eof(QUERY3_ID)
        self.__send_book_eof(QUERY5_ID)

    def __distribute_books(self, chunk_id: int, sharded_chunks: list, query_id: str):
        n_workers = len(sharded_chunks)
        for worker_i in range(1, n_workers+1):
            i = worker_i - 1
            if not sharded_chunks[i]:
                continue
            self.total_books[query_id][str(worker_i)] += len(sharded_chunks[i])
            data_wi = self.book_serializers[query_id].to_bytes(sharded_chunks[i])
            msg = Message(
                ID=chunk_id,
                client_id=self.client_id,
                type=MessageType.DATA,
                data=data_wi,
            )
            self.middleware.produce(
                msg.to_bytes(),
                out_queue_name=OUT_BOOKS_QUEUE(query_id, worker_i)
            )

    def distribute_books(self, chunk_id, chunk):
        if str(chunk_id) == self.total_books[LAST_CHUNK]:
            return

        # Query 1:
        value_grouped_by_title = group_by_key(chunk, self.workers_by_query[QUERY1_ID], lambda b: b.title)
        self.__distribute_books(chunk_id, value_grouped_by_title, QUERY1_ID)

        # Query 2:
        exploded = explode_by_authors(chunk)
        value_grouped_by_author = group_by_key(exploded, self.workers_by_query[QUERY2_ID], lambda b: b.authors[0])
        self.__distribute_books(chunk_id, value_grouped_by_author, QUERY2_ID)

        # Query 3/4:
        value_grouped_by_title = group_by_key(chunk, self.workers_by_query[QUERY3_ID], lambda b: b.title)
        self.__distribute_books(chunk_id, value_grouped_by_title, QUERY3_ID)

        # Query 5:
        value_grouped_by_title = group_by_key(chunk, self.workers_by_query[QUERY5_ID], lambda b: b.title)
        self.__distribute_books(chunk_id, value_grouped_by_title, QUERY5_ID)

        self.total_books[LAST_CHUNK] = str(chunk_id)
        self.total_books.flush()

    def __send_review_eof(self, query_id):
        for worker_i in self.total_reviews[query_id]:
            eof = Message(
                client_id=self.client_id,
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

    def terminate_reviews(self):
        self.__send_review_eof(QUERY3_ID)
        self.__send_review_eof(QUERY5_ID)

    def __distribute_reviews(self, chunk_id: int, sharded_chunks: list, query_id: str):
        n_workers = len(sharded_chunks)
        for worker_i in range(1, n_workers+1):
            i = worker_i - 1
            if not sharded_chunks[i]:
                continue
            self.total_reviews[query_id][str(worker_i)] += len(sharded_chunks[i])
            data_wi = self.review_serializers[query_id].to_bytes(sharded_chunks[i])
            msg = Message(
                ID=chunk_id,
                client_id=self.client_id,
                type=MessageType.DATA,
                data=data_wi,
            )
            self.middleware.produce(
                msg.to_bytes(),
                out_queue_name=OUT_REVIEWS_QUEUE(query_id, worker_i)
            )

    def distribute_reviews(self, chunk_id, chunk):
        if str(chunk_id) == self.total_reviews[LAST_CHUNK]:
            return

        # Query 3/4:
        reviews_grouped_by_title = group_by_key(chunk, self.workers_by_query[QUERY3_ID], lambda r: r.title)
        self.__distribute_reviews(chunk_id, reviews_grouped_by_title, QUERY3_ID)

        # Query 5:
        reviews_grouped_by_author = group_by_key(chunk, self.workers_by_query[QUERY5_ID], lambda r: r.title)
        self.__distribute_reviews(chunk_id, reviews_grouped_by_author, QUERY5_ID)

        self.total_reviews[LAST_CHUNK] = str(chunk_id)
        self.total_reviews.flush()

    def stop(self):
        self.middleware.stop()
