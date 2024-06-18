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


class TestUtils(unittest.TestCase):
    def append_eof(self, client_id, test_middleware, sent):
        eof = Message(
            client_id=client_id,
            type=MessageType.EOF,
            data=b'',
            args={
                TOTAL: sent,
            }
        )
        test_middleware.add_message(eof.to_bytes())

    def append_chunk(self, client_id, test_middleware, chunk):
        serializer = Q1InSerializer()
        msg = Message(
            client_id=client_id,
            type=MessageType.DATA,
            data=serializer.to_bytes(chunk),
        )
        test_middleware.add_message(msg.to_bytes())

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

    def test_worker_filter(self):
        def matches_function(b: Book):
            return 'distributed' in b.title.lower()
        in_serializer = Q1InSerializer()
        out_serializer = Q1OutSerializer()
        client_id = uuid.uuid4()
        test_middleware = TestMiddleware()

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

        _chunk1 = in_serializer.to_bytes([book1, book2])
        msg = Message(
            client_id=client_id,
            type=MessageType.DATA,
            data=_chunk1
        )
        test_middleware.add_message(msg.to_bytes())

        _chunk2 = in_serializer.to_bytes([book3, book4])
        msg = Message(
            client_id=client_id,
            type=MessageType.DATA,
            data=_chunk2
        )
        test_middleware.add_message(msg.to_bytes())

        eof = Message(
            client_id=client_id,
            type=MessageType.EOF,
            data=b'',
            args={
                TOTAL: 4,
            }
        )
        test_middleware.add_message(eof.to_bytes())

        worker = Query1Worker(peer_id=1, peers=10, chunk_size=2, matches=matches_function, test_middleware=test_middleware)
        worker.run()
        sent = [Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent]
        eofs = [msg for msg in sent if msg.type == MessageType.EOF]
        _filtered_chunks = [msg.data for msg in sent if msg.type == MessageType.DATA]
        filtered_chunks = [out_serializer.from_chunk(io.BytesIO(_chunk)) for _chunk in _filtered_chunks]
        filtered = [
            b.title
            for chunk in filtered_chunks
            for b in chunk
        ]

        assert len(eofs) == 1
        assert eofs[0].args[TOTAL] == 3
        assert len(filtered) == 3
        assert book1.title in filtered
        assert book2.title in filtered
        assert book3.title in filtered

    def test_worker_premature_eof(self):
        def matches_function(b: Book):
            return 'distributed' in b.title.lower()
        in_serializer = Q1InSerializer()
        out_serializer = Q1OutSerializer()
        client_id = uuid.uuid4()
        test_middleware = TestMiddleware()

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

        eof = Message(
            client_id=client_id,
            type=MessageType.EOF,
            data=b'',
            args={
                TOTAL: 4,
            }
        )
        test_middleware.add_message(eof.to_bytes())

        _chunk1 = in_serializer.to_bytes([book1, book2])
        msg = Message(
            client_id=client_id,
            type=MessageType.DATA,
            data=_chunk1
        )
        test_middleware.add_message(msg.to_bytes())

        _chunk2 = in_serializer.to_bytes([book3, book4])
        msg = Message(
            client_id=client_id,
            type=MessageType.DATA,
            data=_chunk2
        )
        test_middleware.add_message(msg.to_bytes())

        worker = Query1Worker(peer_id=1, peers=10, chunk_size=2, matches=matches_function, test_middleware=test_middleware)
        worker.run()
        sent = [Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent]
        eofs = [msg for msg in sent if msg.type == MessageType.EOF]
        _filtered_chunks = [msg.data for msg in sent if msg.type == MessageType.DATA]
        filtered_chunks = [out_serializer.from_chunk(io.BytesIO(_chunk)) for _chunk in _filtered_chunks]
        filtered = [
            b.title
            for chunk in filtered_chunks
            for b in chunk
        ]

        assert len(eofs) == 1
        assert eofs[0].args[TOTAL] == 3
        assert len(filtered) == 3
        assert book1.title in filtered
        assert book2.title in filtered
        assert book3.title in filtered

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

    def test_sequential_multiclient(self):
        client_1 = uuid.uuid4()
        client_2 = uuid.uuid4()
        test_middleware = TestMiddleware()
        out_serializer = Q1OutSerializer()

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

        worker = Query1Worker(peer_id=1, peers=10, chunk_size=2, matches=matches_function, test_middleware=test_middleware)
        worker.run()
        sent = [Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent]

        # client 1
        sent_c1 = [msg for msg in sent if msg.client_id == client_1]
        eofs_c1 = [msg for msg in sent_c1 if msg.type == MessageType.EOF]
        _filtered_chunks_c1 = [msg.data for msg in sent_c1 if msg.type == MessageType.DATA]
        filtered_chunks_c1 = [out_serializer.from_chunk(io.BytesIO(_chunk)) for _chunk in _filtered_chunks_c1]
        filtered_c1 = [
            b.title
            for chunk in filtered_chunks_c1
            for b in chunk
        ]
        assert len(eofs_c1) == 1
        assert eofs_c1[0].args[TOTAL] == 2
        assert len(filtered_c1) == 2
        assert b1.title not in filtered_c1
        assert b2.title not in filtered_c1
        assert b3.title not in filtered_c1
        assert b4.title in filtered_c1
        assert b5.title in filtered_c1

        # client 2
        sent_c2 = [msg for msg in sent if msg.client_id == client_2]
        eofs_c2 = [msg for msg in sent_c2 if msg.type == MessageType.EOF]
        _filtered_chunks_c2 = [msg.data for msg in sent_c2 if msg.type == MessageType.DATA]
        filtered_chunks_c2 = [out_serializer.from_chunk(io.BytesIO(_chunk)) for _chunk in _filtered_chunks_c2]
        filtered_c2 = [
            b.title
            for chunk in filtered_chunks_c2
            for b in chunk
        ]
        assert len(eofs_c2) == 1
        assert eofs_c2[0].args[TOTAL] == 3
        assert len(filtered_c2) == 3
        assert c1.title in filtered_c2
        assert c2.title in filtered_c2
        assert c3.title in filtered_c2
        assert c4.title not in filtered_c2

    def test_parallel_multiclient(self):
        client_1 = uuid.uuid4()
        client_2 = uuid.uuid4()
        test_middleware = TestMiddleware()
        out_serializer = Q1OutSerializer()

        b1, b2, b3, b4, b5 = self.make_books_asoiaf()
        c1, c2, c3, c4 = self.make_books_tlotr()

        self.append_chunk(client_1, test_middleware, [b1, b2])
        self.append_chunk(client_2, test_middleware, [c1])
        self.append_chunk(client_1, test_middleware, [b3])
        self.append_chunk(client_2, test_middleware, [c2, c3])
        self.append_eof(client_1, test_middleware, 5)
        self.append_eof(client_2, test_middleware, 4)
        self.append_chunk(client_1, test_middleware, [b4, b5])
        self.append_chunk(client_2, test_middleware, [c4])

        def matches_function(b: Book):
            return int(b.publishedDate) < 1990 or int(b.publishedDate) > 2000

        worker = Query1Worker(peer_id=1, peers=10, chunk_size=2, matches=matches_function, test_middleware=test_middleware)
        worker.run()
        sent = [Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent]

        # client 1
        sent_c1 = [msg for msg in sent if msg.client_id == client_1]
        eofs_c1 = [msg for msg in sent_c1 if msg.type == MessageType.EOF]
        _filtered_chunks_c1 = [msg.data for msg in sent_c1 if msg.type == MessageType.DATA]
        filtered_chunks_c1 = [out_serializer.from_chunk(io.BytesIO(_chunk)) for _chunk in _filtered_chunks_c1]
        filtered_c1 = [
            b.title
            for chunk in filtered_chunks_c1
            for b in chunk
        ]
        assert len(eofs_c1) == 1
        assert eofs_c1[0].args[TOTAL] == 2
        assert len(filtered_c1) == 2
        assert b1.title not in filtered_c1
        assert b2.title not in filtered_c1
        assert b3.title not in filtered_c1
        assert b4.title in filtered_c1
        assert b5.title in filtered_c1

        # client 2
        sent_c2 = [msg for msg in sent if msg.client_id == client_2]
        eofs_c2 = [msg for msg in sent_c2 if msg.type == MessageType.EOF]
        _filtered_chunks_c2 = [msg.data for msg in sent_c2 if msg.type == MessageType.DATA]
        filtered_chunks_c2 = [out_serializer.from_chunk(io.BytesIO(_chunk)) for _chunk in _filtered_chunks_c2]
        filtered_c2 = [
            b.title
            for chunk in filtered_chunks_c2
            for b in chunk
        ]
        assert len(eofs_c2) == 1
        assert eofs_c2[0].args[TOTAL] == 3
        assert len(filtered_c2) == 3
        assert c1.title in filtered_c2
        assert c2.title in filtered_c2
        assert c3.title in filtered_c2
        assert c4.title not in filtered_c2


if __name__ == '__main__':
    unittest.main()
