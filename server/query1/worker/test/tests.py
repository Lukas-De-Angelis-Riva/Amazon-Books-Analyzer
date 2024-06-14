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


if __name__ == '__main__':
    unittest.main()
