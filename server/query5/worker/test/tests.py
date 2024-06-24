import unittest
import shutil
import uuid
import os
import io

from dto.q5Partial import Q5Partial
from model.book import Book
from utils.worker import TOTAL, BASE_DIRECTORY
from model.review import Review
from utils.serializer.q5BookInSerializer import Q5BookInSerializer      # type: ignore
from utils.serializer.q5ReviewInSerializer import Q5ReviewInSerializer  # type: ignore
from utils.serializer.q5PartialSerializer import Q5PartialSerializer    # type: ignore
from utils.model.message import Message, MessageType
from utils.middleware.testMiddleware import TestMiddleware
from common.query5Worker import Query5Worker, IN_BOOKS_QUEUE_NAME, IN_REVIEWS_QUEUE_NAME

from utils.model.virus import virus, Disease

WORKER_ID = 2

SENTIMENT_HIGH = 'THIS BOOK IS AWESOME!! YOU SHOULD BUY IT'
SENTIMENT_NEUTRAL = 'THIS BOOK IS ABOUT DISTRIBUTED SYSTEMS'
SENTIMENT_LOW = 'THIS BOOK IS AWFUL... YOU SHOULDNT BUY IT'


class TestUtils(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.path.exists(BASE_DIRECTORY):
            shutil.rmtree(BASE_DIRECTORY)

    def append_book_eof(self, client_id, test_middleware, sent, eof_id=None):
        eof = Message(
            client_id=client_id,
            type=MessageType.EOF,
            data=b'',
            args={
                TOTAL: sent,
            }
        )
        if eof_id:
            eof.ID = eof_id
        test_middleware.add_message(eof.to_bytes(), IN_BOOKS_QUEUE_NAME(WORKER_ID))

    def append_book_chunk(self, client_id, test_middleware, chunk, chunk_id=None):
        serializer = Q5BookInSerializer()
        msg = Message(
            client_id=client_id,
            type=MessageType.DATA,
            data=serializer.to_bytes(chunk),
        )
        if chunk_id:
            msg.ID = chunk_id
        test_middleware.add_message(msg.to_bytes(), IN_BOOKS_QUEUE_NAME(WORKER_ID))

    def append_review_eof(self, client_id, test_middleware, sent, eof_id=None):
        eof = Message(
            client_id=client_id,
            type=MessageType.EOF,
            data=b'',
            args={
                TOTAL: sent,
            }
        )
        if eof_id:
            eof.ID = eof_id
        test_middleware.add_message(eof.to_bytes(), IN_REVIEWS_QUEUE_NAME(WORKER_ID))

    def append_review_chunk(self, client_id, test_middleware, chunk, chunk_id=None):
        serializer = Q5ReviewInSerializer()
        msg = Message(
            client_id=client_id,
            type=MessageType.DATA,
            data=serializer.to_bytes(chunk),
        )
        if chunk_id:
            msg.ID = chunk_id
        test_middleware.add_message(msg.to_bytes(), IN_REVIEWS_QUEUE_NAME(WORKER_ID))

    def check(self, client_id, ps, sent):
        serializer = Q5PartialSerializer()
        sent = [msg for msg in sent if msg.client_id == client_id]
        eofs = [msg for msg in sent if msg.type == MessageType.EOF]
        _sent_chunks = [msg.data for msg in sent if msg.type == MessageType.DATA]
        sent_chunks = [serializer.from_chunk(io.BytesIO(_chunk)) for _chunk in _sent_chunks]
        sent_ps = [
            p.title
            for chunk in sent_chunks
            for p in chunk
        ]
        assert len(eofs) == 1, f'unexpected amount of EOFs, sent: {eofs}'
        assert eofs[0].args[TOTAL] == len(ps), \
            f'wrong EOF[TOTAL] | exp: {len(ps)}, real: {eofs[0].args[TOTAL]}'

        assert len(sent_ps) == len(ps), \
            f'wrong len(sent) | exp: {len(ps)}, real: {len(sent_ps)}'
        for a in ps:
            assert a in sent_ps, f'{a} not in {sent_ps}'

    def test_empty_partialq3_update_correctly(self):
        partial = Q5Partial(
            title='The C programming language',
            n=0,
            sentimentAvg=0.0,
        )
        new_review = Review(
            id=0,
            title='The C programming language',
            score=5.0,
            text='A really good manual'
        )
        partial.update(new_review)

        assert partial.n == 1
        assert partial.sentimentAvg > 0

    def test_partialq3_update_correctly(self):
        partial = Q5Partial(
            title='The C programming language',
            n=10,
            sentimentAvg=0.0,
        )
        new_review = Review(
            id=0,
            title='The C programming language',
            score=5.0,
            text='A really good manual'
        )
        partial.update(new_review)

        assert partial.n == 11
        assert partial.sentimentAvg > 0

    def test_sentiment_function(self):
        partial1 = Q5Partial(
            title='The C programming language',
            n=0,
            sentimentAvg=0.0,
        )
        review1 = Review(
            id=0,
            title='The C programming language',
            score=5.0,
            text='A really good manual'
        )
        partial1.update(review1)

        partial2 = Q5Partial(
            title='The C programming language',
            n=0,
            sentimentAvg=0.0,
        )
        review2 = Review(
            id=0,
            title='The C programming language',
            score=5.0,
            text='This manual is useless'
        )
        partial2.update(review2)

        assert partial1.sentimentAvg > partial2.sentimentAvg

    def test_partialq3_merge_correctly(self):
        partial1 = Q5Partial(
            title='The C programming language',
            n=10,
            sentimentAvg=4.5,
        )
        partial2 = Q5Partial(
            title='The C programming language',
            n=5,
            sentimentAvg=5.0,
        )

        partial1.merge(partial2)

        assert partial1.n == 10+5
        assert abs(partial1.sentimentAvg - 14/3) < 1e-4

    def test_partialq3serializer(self):
        serializer = Q5PartialSerializer()

        partial1 = Q5Partial(
            title='The C programming language',
            n=10,
            sentimentAvg=4.5,
        )
        partial2 = Q5Partial(
            title='The C programming language',
            n=5,
            sentimentAvg=5.0,
        )

        chunk = serializer.to_bytes([partial1, partial2])
        reader = io.BytesIO(chunk)
        serial = serializer.from_chunk(reader)

        _partial1 = serial[0]
        _partial2 = serial[1]

        assert partial1.title == _partial1.title
        assert partial1.n == _partial1.n
        assert abs(partial1.sentimentAvg - _partial1.sentimentAvg) < 1e-4

        assert partial2.title == _partial2.title
        assert partial2.n == _partial2.n
        assert abs(partial2.sentimentAvg - _partial2.sentimentAvg) < 1e-4

    def make_books_distributed(self):
        book1 = Book(
            title='Distributed Systems: Concepts and Design, 5th Edition',
            authors=['G. Coulouris', 'J. Dollimore', 't. Kindberg', 'G. Blair'],
            publisher='Addison Wesley',
            publishedDate='2012',
            categories=['Computers', 'Distributed Systems'],
        )

        book2 = Book(
            title='Distributed Systems, 3rd Edition',
            authors=['M. Van Steen', 'A. Tanenbaum'],
            publisher='Pearson Education',
            publishedDate='2014',
            categories=['Computers', 'Distributed Systems'],
        )

        book3 = Book(
            title='Distributed Systems for Systems Architects',
            authors=['P. Verissimo', 'L. Rodriguez'],
            publisher='Kluwer Academic Publishers',
            publishedDate='2001',
            categories=['Computers', 'Distributed Systems'],
        )

        book4 = Book(
            title='Designing Data-Intensive Applications',
            authors=['Martin Kleppmann'],
            publisher='O Reilly Media',
            publishedDate='2020',
            categories=['Computers'],
        )
        return book1, book2, book3, book4

    def make_reviews(self, book, n, text):
        reviews = []
        for i in range(n):
            r = Review(id=f'id{i}', title=book.title, score=0, text=text)
            reviews.append(r)
        return reviews

    def test_worker(self):
        client_id = uuid.UUID('00000000-0000-0000-0000-000000000000')
        test_middleware = TestMiddleware()
        b1, b2, b3, b4 = self.make_books_distributed()
        rs1 = self.make_reviews(b1, 2, SENTIMENT_HIGH)
        rs2 = self.make_reviews(b2, 4, SENTIMENT_LOW)
        rs3 = []    # no reviews about b3
        rs4 = self.make_reviews(b4, 2, SENTIMENT_NEUTRAL)
        rs = rs1+rs2+rs3+rs4

        self.append_book_chunk(client_id, test_middleware, [b1])
        self.append_book_chunk(client_id, test_middleware, [b2])
        self.append_book_chunk(client_id, test_middleware, [b3, b4])
        self.append_book_eof(client_id, test_middleware, sent=4)

        self.append_review_chunk(client_id, test_middleware, [rs1[0], rs2[3]])
        self.append_review_chunk(client_id, test_middleware, [rs2[2], rs2[1]])
        self.append_review_chunk(client_id, test_middleware, [rs2[0], rs4[0]])
        self.append_review_chunk(client_id, test_middleware, [rs1[1], rs4[1]])
        self.append_review_eof(client_id, test_middleware, sent=len(rs))

        worker = Query5Worker(category='Distributed Systems', peer_id=WORKER_ID, peers=10, chunk_size=2,
                              test_middleware=test_middleware)
        worker.run()

        sent = set([Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent])
        self.check(client_id, [b1.title, b2.title], sent)

    def test_worker_premature_eof(self):
        client_id = uuid.UUID('10000000-0000-0000-0000-000000000000')
        test_middleware = TestMiddleware()
        b1, b2, b3, b4 = self.make_books_distributed()
        rs1 = self.make_reviews(b1, 2, SENTIMENT_HIGH)
        rs2 = self.make_reviews(b2, 4, SENTIMENT_LOW)
        rs3 = []    # no reviews about b3
        rs4 = self.make_reviews(b4, 2, SENTIMENT_NEUTRAL)
        rs = rs1+rs2+rs3+rs4

        self.append_review_chunk(client_id, test_middleware, [rs1[0], rs2[3]])
        self.append_book_eof(client_id, test_middleware, sent=4)
        self.append_review_chunk(client_id, test_middleware, [rs2[2], rs2[1]])
        self.append_review_eof(client_id, test_middleware, sent=len(rs))
        self.append_book_chunk(client_id, test_middleware, [b1])
        self.append_book_chunk(client_id, test_middleware, [b2])
        self.append_review_chunk(client_id, test_middleware, [rs2[0], rs4[0]])
        self.append_review_chunk(client_id, test_middleware, [rs1[1], rs4[1]])
        self.append_book_chunk(client_id, test_middleware, [b3, b4])

        worker = Query5Worker(category='Distributed Systems', peer_id=WORKER_ID, peers=10, chunk_size=2,
                              test_middleware=test_middleware)
        worker.run()

        sent = set([Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent])
        self.check(client_id, [b1.title, b2.title], sent)

    def test_sequential_multiclient(self):
        client_1 = uuid.UUID('20000000-0000-0000-0000-000000000000')
        client_2 = uuid.UUID('21000000-0000-0000-0000-000000000000')
        client_3 = uuid.UUID('22000000-0000-0000-0000-000000000000')
        test_middleware = TestMiddleware()
        b1, b2, b3, b4 = self.make_books_distributed()

        rs1_1 = self.make_reviews(b1, 2, SENTIMENT_HIGH)
        rs2_1 = self.make_reviews(b2, 4, SENTIMENT_LOW)
        rs3_1 = []    # no reviews about b3
        rs4_1 = self.make_reviews(b4, 2, SENTIMENT_NEUTRAL)
        rs_1 = rs1_1+rs2_1+rs3_1+rs4_1

        rs1_2 = self.make_reviews(b1, 4, SENTIMENT_HIGH)
        rs2_2 = []    # no reviews about b2
        rs3_2 = []    # no reviews about b3
        rs4_2 = self.make_reviews(b4, 2, SENTIMENT_NEUTRAL)
        rs_2 = rs1_2+rs2_2+rs3_2+rs4_2

        rs1_3 = self.make_reviews(b1, 2, SENTIMENT_HIGH)
        rs2_3 = self.make_reviews(b2, 2, SENTIMENT_NEUTRAL)
        rs3_3 = self.make_reviews(b3, 2, SENTIMENT_LOW)
        rs4_3 = self.make_reviews(b4, 2, SENTIMENT_NEUTRAL)
        rs_3 = rs1_3+rs2_3+rs3_3+rs4_3

        # CLIENT 1
        self.append_book_chunk(client_1, test_middleware, [b1])
        self.append_book_chunk(client_1, test_middleware, [b2])
        self.append_book_chunk(client_1, test_middleware, [b3, b4])
        self.append_book_eof(client_1, test_middleware, sent=4)

        self.append_review_chunk(client_1, test_middleware, [rs1_1[0], rs2_1[3]])
        self.append_review_chunk(client_1, test_middleware, [rs2_1[2], rs2_1[1]])
        self.append_review_chunk(client_1, test_middleware, [rs2_1[0], rs4_1[0]])
        self.append_review_chunk(client_1, test_middleware, [rs1_1[1], rs4_1[1]])
        self.append_review_eof(client_1, test_middleware, sent=len(rs_1))

        # CLIENT 2
        self.append_book_chunk(client_2, test_middleware, [b1, b4])
        self.append_book_chunk(client_2, test_middleware, [b2, b3])
        self.append_book_eof(client_2, test_middleware, sent=4)

        self.append_review_chunk(client_2, test_middleware, [rs1_2[0], rs1_2[1], rs4_2[0]])
        self.append_review_chunk(client_2, test_middleware, [rs1_2[2]])
        self.append_review_chunk(client_2, test_middleware, [rs1_2[3], rs4_2[1]])
        self.append_review_eof(client_2, test_middleware, sent=len(rs_2))

        # CLIENT 3
        self.append_book_chunk(client_3, test_middleware, [b1])
        self.append_book_chunk(client_3, test_middleware, [b2])
        self.append_book_chunk(client_3, test_middleware, [b3])
        self.append_book_chunk(client_3, test_middleware, [b4])
        self.append_book_eof(client_3, test_middleware, sent=4)

        self.append_review_chunk(client_3, test_middleware, [rs1_3[0], rs3_3[1]])
        self.append_review_chunk(client_3, test_middleware, [rs2_3[0], rs4_3[1]])
        self.append_review_chunk(client_3, test_middleware, [rs3_3[0], rs2_3[1]])
        self.append_review_chunk(client_3, test_middleware, [rs4_3[0], rs1_3[1]])
        self.append_review_eof(client_3, test_middleware, sent=len(rs_3))

        worker = Query5Worker(category='Distributed Systems', peer_id=WORKER_ID, peers=10, chunk_size=2,
                              test_middleware=test_middleware)
        worker.run()

        sent = set([Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent])
        self.check(client_1, [b1.title, b2.title], sent)
        self.check(client_2, [b1.title], sent)
        self.check(client_3, [b1.title, b2.title, b3.title], sent)

    def test_parallel_multiclient(self):
        client_1 = uuid.UUID('30000000-0000-0000-0000-000000000000')
        client_2 = uuid.UUID('31000000-0000-0000-0000-000000000000')
        client_3 = uuid.UUID('32000000-0000-0000-0000-000000000000')
        test_middleware = TestMiddleware()
        b1, b2, b3, b4 = self.make_books_distributed()

        rs1_1 = self.make_reviews(b1, 2, SENTIMENT_HIGH)
        rs2_1 = self.make_reviews(b2, 4, SENTIMENT_LOW)
        rs3_1 = []    # no reviews about b3
        rs4_1 = self.make_reviews(b4, 2, SENTIMENT_NEUTRAL)
        rs_1 = rs1_1+rs2_1+rs3_1+rs4_1

        rs1_2 = self.make_reviews(b1, 4, SENTIMENT_HIGH)
        rs2_2 = []    # no reviews about b2
        rs3_2 = []    # no reviews about b3
        rs4_2 = self.make_reviews(b4, 2, SENTIMENT_NEUTRAL)
        rs_2 = rs1_2+rs2_2+rs3_2+rs4_2

        rs1_3 = self.make_reviews(b1, 2, SENTIMENT_HIGH)
        rs2_3 = self.make_reviews(b2, 2, SENTIMENT_NEUTRAL)
        rs3_3 = self.make_reviews(b3, 2, SENTIMENT_LOW)
        rs4_3 = self.make_reviews(b4, 2, SENTIMENT_NEUTRAL)
        rs_3 = rs1_3+rs2_3+rs3_3+rs4_3

        # -- -- -- -- CHAOS -- -- -- --
        self.append_review_chunk(client_2, test_middleware, [rs1_2[3], rs4_2[1]])
        self.append_review_chunk(client_2, test_middleware, [rs1_2[0], rs1_2[1], rs4_2[0]])
        self.append_book_eof(client_2, test_middleware, sent=4)
        self.append_book_chunk(client_3, test_middleware, [b4])
        self.append_review_chunk(client_2, test_middleware, [rs1_2[2]])
        self.append_review_eof(client_2, test_middleware, sent=len(rs_2))
        self.append_review_chunk(client_3, test_middleware, [rs3_3[0], rs2_3[1]])
        self.append_review_chunk(client_3, test_middleware, [rs1_3[0], rs3_3[1]])
        self.append_book_eof(client_3, test_middleware, sent=4)
        self.append_review_chunk(client_1, test_middleware, [rs1_1[1], rs4_1[1]])
        self.append_book_chunk(client_3, test_middleware, [b2])
        self.append_book_eof(client_1, test_middleware, sent=4)
        self.append_review_chunk(client_3, test_middleware, [rs4_3[0], rs1_3[1]])
        self.append_book_chunk(client_1, test_middleware, [b2])
        self.append_book_chunk(client_3, test_middleware, [b1])
        self.append_review_eof(client_3, test_middleware, sent=len(rs_3))
        self.append_book_chunk(client_1, test_middleware, [b3, b4])
        self.append_book_chunk(client_1, test_middleware, [b1])
        self.append_book_chunk(client_2, test_middleware, [b1, b4])
        self.append_book_chunk(client_3, test_middleware, [b3])
        self.append_review_chunk(client_1, test_middleware, [rs2_1[2], rs2_1[1]])
        self.append_review_chunk(client_1, test_middleware, [rs1_1[0], rs2_1[3]])
        self.append_review_eof(client_1, test_middleware, sent=len(rs_1))
        self.append_review_chunk(client_3, test_middleware, [rs2_3[0], rs4_3[1]])
        self.append_review_chunk(client_1, test_middleware, [rs2_1[0], rs4_1[0]])
        self.append_book_chunk(client_2, test_middleware, [b2, b3])

        worker = Query5Worker(category='Distributed Systems', peer_id=WORKER_ID, peers=10, chunk_size=2,
                              test_middleware=test_middleware)
        worker.run()

        sent = set([Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent])
        self.check(client_1, [b1.title, b2.title], sent)
        self.check(client_2, [b1.title], sent)
        self.check(client_3, [b1.title, b2.title, b3.title], sent)

    def test_infected_worker(self):
        client_id = uuid.UUID('40000000-0000-0000-0000-000000000000')
        test_middleware = TestMiddleware()
        b1, b2, b3, b4 = self.make_books_distributed()
        rs1 = self.make_reviews(b1, 2, SENTIMENT_HIGH)
        rs2 = self.make_reviews(b2, 4, SENTIMENT_LOW)
        rs3 = []    # no reviews about b3
        rs4 = self.make_reviews(b4, 2, SENTIMENT_NEUTRAL)
        rs = rs1+rs2+rs3+rs4

        self.append_book_chunk(client_id, test_middleware, [b1])
        self.append_book_chunk(client_id, test_middleware, [b2])
        self.append_book_chunk(client_id, test_middleware, [b3, b4])
        self.append_book_eof(client_id, test_middleware, sent=4)

        self.append_review_chunk(client_id, test_middleware, [rs1[0], rs2[3]])
        self.append_review_chunk(client_id, test_middleware, [rs2[2], rs2[1]])
        self.append_review_chunk(client_id, test_middleware, [rs2[0], rs4[0]])
        self.append_review_chunk(client_id, test_middleware, [rs1[1], rs4[1]])
        self.append_review_eof(client_id, test_middleware, sent=len(rs))

        virus.mutate(0.20)
        virus.disease_counter = 0
        while True:
            try:
                worker = Query5Worker(category='Distributed Systems', peer_id=WORKER_ID, peers=10, chunk_size=2,
                                      test_middleware=test_middleware)
                worker.run()
                break
            except Disease:
                continue
        virus.mutate(0)
        print(f"UNIQUE-CLIENT | DISEASE COUNTER: {virus.disease_counter}")

        sent = set([Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent])
        self.check(client_id, [b1.title, b2.title], sent)

    def test_infected_worker_parallel_multiclient(self):
        client_1 = uuid.UUID('50000000-0000-0000-0000-000000000000')
        client_2 = uuid.UUID('51000000-0000-0000-0000-000000000000')
        client_3 = uuid.UUID('52000000-0000-0000-0000-000000000000')
        test_middleware = TestMiddleware()
        b1, b2, b3, b4 = self.make_books_distributed()

        rs1_1 = self.make_reviews(b1, 2, SENTIMENT_HIGH)
        rs2_1 = self.make_reviews(b2, 4, SENTIMENT_LOW)
        rs3_1 = []    # no reviews about b3
        rs4_1 = self.make_reviews(b4, 2, SENTIMENT_NEUTRAL)
        rs_1 = rs1_1+rs2_1+rs3_1+rs4_1

        rs1_2 = self.make_reviews(b1, 4, SENTIMENT_HIGH)
        rs2_2 = []    # no reviews about b2
        rs3_2 = []    # no reviews about b3
        rs4_2 = self.make_reviews(b4, 2, SENTIMENT_NEUTRAL)
        rs_2 = rs1_2+rs2_2+rs3_2+rs4_2

        rs1_3 = self.make_reviews(b1, 2, SENTIMENT_HIGH)
        rs2_3 = self.make_reviews(b2, 2, SENTIMENT_NEUTRAL)
        rs3_3 = self.make_reviews(b3, 2, SENTIMENT_LOW)
        rs4_3 = self.make_reviews(b4, 2, SENTIMENT_NEUTRAL)
        rs_3 = rs1_3+rs2_3+rs3_3+rs4_3

        # -- -- -- -- CHAOS -- -- -- --
        self.append_review_chunk(client_2, test_middleware, [rs1_2[3], rs4_2[1]])
        self.append_review_chunk(client_2, test_middleware, [rs1_2[0], rs1_2[1], rs4_2[0]])
        self.append_book_eof(client_2, test_middleware, sent=4)
        self.append_book_chunk(client_3, test_middleware, [b4])
        self.append_review_chunk(client_2, test_middleware, [rs1_2[2]])
        self.append_review_eof(client_2, test_middleware, sent=len(rs_2))
        self.append_review_chunk(client_3, test_middleware, [rs3_3[0], rs2_3[1]])
        self.append_review_chunk(client_3, test_middleware, [rs1_3[0], rs3_3[1]])
        self.append_book_eof(client_3, test_middleware, sent=4)
        self.append_review_chunk(client_1, test_middleware, [rs1_1[1], rs4_1[1]])
        self.append_book_chunk(client_3, test_middleware, [b2])
        self.append_book_eof(client_1, test_middleware, sent=4)
        self.append_review_chunk(client_3, test_middleware, [rs4_3[0], rs1_3[1]])
        self.append_book_chunk(client_1, test_middleware, [b2])
        self.append_book_chunk(client_3, test_middleware, [b1])
        self.append_review_eof(client_3, test_middleware, sent=len(rs_3))
        self.append_book_chunk(client_1, test_middleware, [b3, b4])
        self.append_book_chunk(client_1, test_middleware, [b1])
        self.append_book_chunk(client_2, test_middleware, [b1, b4])
        self.append_book_chunk(client_3, test_middleware, [b3])
        self.append_review_chunk(client_1, test_middleware, [rs2_1[2], rs2_1[1]])
        self.append_review_chunk(client_1, test_middleware, [rs1_1[0], rs2_1[3]])
        self.append_review_eof(client_1, test_middleware, sent=len(rs_1))
        self.append_review_chunk(client_3, test_middleware, [rs2_3[0], rs4_3[1]])
        self.append_review_chunk(client_1, test_middleware, [rs2_1[0], rs4_1[0]])
        self.append_book_chunk(client_2, test_middleware, [b2, b3])

        virus.mutate(0.10)
        virus.disease_counter = 0
        while True:
            try:
                worker = Query5Worker(category='Distributed Systems', peer_id=WORKER_ID, peers=10, chunk_size=2,
                                      test_middleware=test_middleware)
                worker.run()
                break
            except Disease:
                continue
        virus.mutate(0)
        print(f"UNIQUE-CLIENT | DISEASE COUNTER: {virus.disease_counter}")

        sent = set([Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent])
        self.check(client_1, [b1.title, b2.title], sent)
        self.check(client_2, [b1.title], sent)
        self.check(client_3, [b1.title, b2.title, b3.title], sent)


if __name__ == '__main__':
    unittest.main()
