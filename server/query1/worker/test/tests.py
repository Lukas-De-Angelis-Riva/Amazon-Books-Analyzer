import io
import unittest
import uuid

from model.book import Book
from utils.serializer.q1InSerializer import Q1InSerializer      # type: ignore
from utils.serializer.q1OutSerializer import Q1OutSerializer    # type: ignore
from common.query1Worker import Query1Worker
from utils.worker import TOTAL
from utils.middleware.testMiddleware import TestMiddleware
from utils.model.message import Message, MessageType
from utils.model.virus import Disease, virus


class TestUtils(unittest.TestCase):
    def append_eof(self, client_id, test_middleware, sent, eof_id=None):
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

        test_middleware.add_message(eof.to_bytes())

    def append_chunk(self, client_id, test_middleware, chunk, chunk_id=None):
        serializer = Q1InSerializer()
        msg = Message(
            client_id=client_id,
            type=MessageType.DATA,
            data=serializer.to_bytes(chunk),
        )
        if chunk_id:
            msg.ID = chunk_id
        test_middleware.add_message(msg.to_bytes())

    def check(self, client_id, books, sent):
        serializer = Q1OutSerializer()
        sent = [msg for msg in sent if msg.client_id == client_id]
        eofs = [msg for msg in sent if msg.type == MessageType.EOF]
        _filtered_chunks = [msg.data for msg in sent if msg.type == MessageType.DATA]
        filtered_chunks = [serializer.from_chunk(io.BytesIO(_chunk)) for _chunk in _filtered_chunks]
        filtered = [
            b.title
            for chunk in filtered_chunks
            for b in chunk
        ]
        assert len(eofs) == 1, f'unexpected amount of EOFs, sent: {eofs}'
        assert eofs[0].args[TOTAL] == len(books), \
            f'wrong EOF[TOTAL] | exp: {len(books)}, real: {eofs[0].args[TOTAL]}'

        assert len(filtered) == len(books), \
            f'wrong len(sent) | exp: {len(books)}, real: {len(filtered)}'

        for b in books:
            assert b.title in filtered, f'{b.title} not in {filtered}'

    def test_in_serializer(self):
        serializer = Q1InSerializer()

        book1 = Book(
            title='At the Montains of madness',
            authors=['H.P. Lovecraft'],
            publisher='Astounding Stories',
            publishedDate='1936',
            categories=['Science fiction', 'Horror'],
        )

        book2 = Book(
            title='The C programming language',
            authors=['Dennis Ritchie', 'Brian Kernighan'],
            publisher='Prentice Hall',
            publishedDate='1978',
            categories=['Programming', 'Manual'],
        )

        chunk = serializer.to_bytes([book1, book2])
        reader = io.BytesIO(chunk)
        serial = serializer.from_chunk(reader)

        _book1 = serial[0]
        _book2 = serial[1]

        assert book1.title == _book1.title
        assert book1.authors == _book1.authors
        assert book1.publisher == _book1.publisher
        assert book1.publishedDate == _book1.publishedDate
        assert book1.categories == _book1.categories

        assert book2.title == _book2.title
        assert book2.authors == _book2.authors

        assert book2.publisher == _book2.publisher
        assert book2.publishedDate == _book2.publishedDate
        assert book2.categories == _book2.categories

    def test_out_serializer(self):
        serializer = Q1OutSerializer()

        book1 = Book(
            title='At the Montains of madness',
            authors=['H.P. Lovecraft'],
            publisher='Astounding Stories',
            publishedDate='',
            categories=[],
        )

        book2 = Book(
            title='The C programming language',
            authors=['Dennis Ritchie', 'Brian Kernighan'],
            publisher='Prentice Hall',
            publishedDate='',
            categories=[],
        )

        chunk = serializer.to_bytes([book1, book2])
        reader = io.BytesIO(chunk)
        serial = serializer.from_chunk(reader)

        _book1 = serial[0]
        _book2 = serial[1]

        assert book1.title == _book1.title
        assert book1.authors == _book1.authors
        assert book1.publisher == _book1.publisher

        assert book2.title == _book2.title
        assert book2.authors == _book2.authors
        assert book2.publisher == _book2.publisher

    def make_books_distributed(self):
        book1 = Book(
            title='Distributed Systems: Concepts and Design, 5th Edition',
            authors=['G. Coulouris', 'J. Dollimore', 't. Kindberg', 'G. Blair'],
            publisher='Addison Wesley',
            publishedDate='2012',
            categories=['Computers'],
        )

        book2 = Book(
            title='Distributed Systems, 3rd Edition',
            authors=['M. Van Steen', 'A. Tanenbaum'],
            publisher='Pearson Education',
            publishedDate='2017',
            categories=['Computers'],
        )

        book3 = Book(
            title='Distributed Systems for Systems Architects',
            authors=['P. Verissimo', 'L. Rodriguez'],
            publisher='Kluwer Academic Publishers',
            publishedDate='2001',
            categories=['Computers'],
        )

        book4 = Book(
            title='Designing Data-Intensive Applications',
            authors=['Martin Kleppmann'],
            publisher='O Reilly Media',
            publishedDate='2017',
            categories=['Computers'],
        )
        return book1, book2, book3, book4

    def make_books_asoiaf(self):
        agot = Book(
            title='A Game of Thrones',
            authors=['George R. R. Martin'],
            publisher='Voyager Books',
            publishedDate='1996',
            categories=['Political Novel', 'Epic Fantasy'],
        )

        acok = Book(
            title='A Clash of Kings',
            authors=['George R. R. Martin'],
            publisher='Voyager Books',
            publishedDate='1999',
            categories=['Political Novel', 'Epic Fantasy'],
        )

        asos = Book(
            title='A Storm of Swords',
            authors=['George R. R. Martin'],
            publisher='Voyager Books',
            publishedDate='2000',
            categories=['Political Novel', 'Epic Fantasy'],
        )
        affc = Book(
            title='A Feast for Crows',
            authors=['George R. R. Martin'],
            publisher='Voyager Books',
            publishedDate='2005',
            categories=['Political Novel', 'Epic Fantasy'],
        )

        adwd = Book(
            title='A Dance with Dragons',
            authors=['George R. R. Martin'],
            publisher='Voyager Books',
            publishedDate='2011',
            categories=['Political Novel', 'Epic Fantasy'],
        )

        return agot, acok, asos, affc, adwd

    def make_books_tlotr(self):

        the_hobbit = Book(
            title='The Hobbit',
            authors=['J. R. R. Tolkien'],
            publisher='George Allen & Unwin',
            publishedDate='1937',
            categories=['Juvenile fantasy', 'High fantasy', 'Epic Fantasy'],
        )
        the_lotr = Book(
            title='The Lord of the Rings',
            authors=['J. R. R. Tolkien'],
            publisher='George Allen & Unwin',
            publishedDate='1954',
            categories=['High fantasy', 'Epic Fantasy'],
        )
        the_aotb = Book(
            title='The Adventures of Tom Bombadil',
            authors=['J. R. R. Tolkien'],
            publisher='George Allen & Unwin',
            publishedDate='1962',
            categories=['Poetry', 'Epic Fantasy'],
        )
        the_simlmarillion = Book(
            title='The Silmarillion',
            authors=['J. R. R. Tolkien'],
            publisher='George Allen & Unwin',
            publishedDate='1994',
            categories=['Mythopoeia', 'Fantasy', 'Epic Fantasy'],
        )
        return the_hobbit, the_lotr, the_aotb, the_simlmarillion

    def test_worker_filter(self):
        def matches_function(b: Book):
            return 'distributed' in b.title.lower()
        client_id = uuid.UUID('00000000-0000-0000-0000-000000000000')
        test_middleware = TestMiddleware()

        b1, b2, b3, b4 = self.make_books_distributed()

        self.append_chunk(client_id, test_middleware, [b1, b4])
        self.append_chunk(client_id, test_middleware, [b3])
        self.append_chunk(client_id, test_middleware, [b2])
        self.append_eof(client_id, test_middleware, 4)

        worker = Query1Worker(1, 10, 2, matches_function, test_middleware=test_middleware)
        worker.run()

        sent = set([Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent])
        self.check(client_id, [b1, b2, b3], sent)

    def test_worker_premature_eof(self):
        def matches_function(b: Book):
            return 'distributed' in b.title.lower()
        client_id = uuid.UUID('10000000-0000-0000-0000-000000000000')
        test_middleware = TestMiddleware()

        b1, b2, b3, b4 = self.make_books_distributed()

        self.append_eof(client_id, test_middleware, 4)
        self.append_chunk(client_id, test_middleware, [b1, b4])
        self.append_chunk(client_id, test_middleware, [b3])
        self.append_chunk(client_id, test_middleware, [b2])

        worker = Query1Worker(1, 10, 2, matches_function, test_middleware=test_middleware)
        worker.run()

        sent = set([Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent])
        self.check(client_id, [b1, b2, b3], sent)

    def test_sequential_multiclient(self):
        client_1 = uuid.UUID('20000000-0000-0000-0000-000000000000')
        client_2 = uuid.UUID('21000000-0000-0000-0000-000000000000')
        test_middleware = TestMiddleware()

        b1, b2, b3, b4, b5 = self.make_books_asoiaf()
        self.append_chunk(client_1, test_middleware, [b1, b2])
        self.append_chunk(client_1, test_middleware, [b3])
        self.append_chunk(client_1, test_middleware, [b4, b5])
        self.append_eof(client_1, test_middleware, 5)

        c1, c2, c3, c4 = self.make_books_tlotr()
        self.append_chunk(client_2, test_middleware, [c1])
        self.append_chunk(client_2, test_middleware, [c2, c3])
        self.append_chunk(client_2, test_middleware, [c4])
        self.append_eof(client_2, test_middleware, 4)

        def matches_function(b: Book):
            return int(b.publishedDate) < 1990 or int(b.publishedDate) > 2000

        worker = Query1Worker(1, 10, 2, matches_function, test_middleware=test_middleware)
        worker.run()
        sent = set([Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent])
        self.check(client_1, [b4, b5], sent)
        self.check(client_2, [c1, c2, c3], sent)

    def test_parallel_multiclient(self):
        client_1 = uuid.UUID('30000000-0000-0000-0000-000000000000')
        client_2 = uuid.UUID('31000000-0000-0000-0000-000000000000')
        test_middleware = TestMiddleware()

        b1, b2, b3, b4, b5 = self.make_books_asoiaf()
        c1, c2, c3, c4 = self.make_books_tlotr()

        # -- -- -- -- --  CHAOS -- -- -- -- --
        self.append_chunk(client_2, test_middleware, [c1])
        self.append_eof(client_1, test_middleware, 5)
        self.append_eof(client_2, test_middleware, 4)
        self.append_chunk(client_1, test_middleware, [b4, b5])
        self.append_chunk(client_2, test_middleware, [c2, c3])
        self.append_chunk(client_1, test_middleware, [b3])
        self.append_chunk(client_2, test_middleware, [c4])
        self.append_chunk(client_1, test_middleware, [b1, b2])

        def matches_function(b: Book):
            return int(b.publishedDate) < 1990 or int(b.publishedDate) > 2000

        worker = Query1Worker(1, 10, 2, matches_function, test_middleware=test_middleware)
        worker.run()

        sent = set([Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent])
        self.check(client_1, [b4, b5], sent)
        self.check(client_2, [c1, c2, c3], sent)

    def test_infected_worker(self):
        def matches_function(b: Book):
            return 'distributed' in b.title.lower()
        client_id = uuid.UUID('40000000-0000-0000-0000-000000000000')
        test_middleware = TestMiddleware()

        b1, b2, b3, b4 = self.make_books_distributed()

        self.append_chunk(client_id, test_middleware, [b1, b4])
        self.append_chunk(client_id, test_middleware, [b3])
        self.append_chunk(client_id, test_middleware, [b2])
        self.append_eof(client_id, test_middleware, 4)

        virus.mutate(0.20)
        while True:
            try:
                worker = Query1Worker(1, 10, 2, matches_function, test_middleware=test_middleware)
                worker.run()
                break
            except Disease:
                test_middleware.requeue()
                continue
        virus.mutate(0)

        sent = set([Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent])
        self.check(client_id, [b1, b2, b3], sent)

    def test_infected_worker_parallel_multiclient(self):
        client_1 = uuid.UUID('50000000-0000-0000-0000-000000000000')
        client_2 = uuid.UUID('51000000-0000-0000-0000-000000000000')
        test_middleware = TestMiddleware()

        b1, b2, b3, b4, b5 = self.make_books_asoiaf()
        c1, c2, c3, c4 = self.make_books_tlotr()

        # -- -- -- -- --  CHAOS -- -- -- -- --
        self.append_chunk(client_2, test_middleware, [c1])
        self.append_eof(client_1, test_middleware, 5)
        self.append_eof(client_2, test_middleware, 4)
        self.append_chunk(client_1, test_middleware, [b4, b5])
        self.append_chunk(client_2, test_middleware, [c2, c3])
        self.append_chunk(client_1, test_middleware, [b3])
        self.append_chunk(client_2, test_middleware, [c4])
        self.append_chunk(client_1, test_middleware, [b1, b2])

        def matches_function(b: Book):
            return int(b.publishedDate) < 1990 or int(b.publishedDate) > 2000

        virus.mutate(0.10)
        while True:
            try:
                worker = Query1Worker(1, 10, 2, matches_function, test_middleware=test_middleware)
                worker.run()
                break
            except Disease:
                test_middleware.requeue()
                continue
        virus.mutate(0)

        sent = set([Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent])
        self.check(client_1, [b4, b5], sent)
        self.check(client_2, [c1, c2, c3], sent)


if __name__ == '__main__':
    unittest.main()
