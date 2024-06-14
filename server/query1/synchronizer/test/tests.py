import io
import unittest
import uuid

from model.book import Book
from utils.serializer.q1OutSerializer import Q1OutSerializer        # type: ignore
from common.query1Synchronizer import Query1Synchronizer
from utils.worker import TOTAL, WORKER_ID
from utils.middleware.testMiddleware import TestMiddleware
from utils.model.message import Message, MessageType


class TestUtils(unittest.TestCase):

    def append_eof(self, client_id, test_middleware, peer_id, sent):
        eof = Message(
            client_id=client_id,
            type=MessageType.EOF,
            data=b'',
            args={
                TOTAL: sent,
                WORKER_ID: peer_id,
            }
        )
        test_middleware.add_message(eof.to_bytes())

    def append_chunk(self, client_id, test_middleware, peer_id, chunk):
        serializer = Q1OutSerializer()
        msg = Message(
            client_id=client_id,
            type=MessageType.DATA,
            data=serializer.to_bytes(chunk),
            args={
                WORKER_ID: peer_id,
            }
        )
        test_middleware.add_message(msg.to_bytes())

    def make_4_books(self):
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

    def test_synchronizer_in_order(self):
        client_id = uuid.uuid4()
        test_middleware = TestMiddleware()
        serializer = Q1OutSerializer()

        b1, b2, b3, b4 = self.make_4_books()
        self.append_chunk(client_id, test_middleware, 1, [b1, b2])
        self.append_chunk(client_id, test_middleware, 2, [b3])
        # -- -- -- -- Worker 3 does not send books -- -- -- --
        self.append_chunk(client_id, test_middleware, 4, [b4])

        self.append_eof(client_id, test_middleware, 1, len([b1, b2]))
        self.append_eof(client_id, test_middleware, 2, len([b3]))
        self.append_eof(client_id, test_middleware, 3, len([]))
        self.append_eof(client_id, test_middleware, 4, len([b4]))

        worker = Query1Synchronizer(n_workers=4, test_middleware=test_middleware)
        worker.run()

        sent = [Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent]
        eofs = [msg for msg in sent if msg.type == MessageType.EOF]
        _filtered_chunks = [msg.data for msg in sent if msg.type == MessageType.DATA]
        filtered_chunks = [serializer.from_chunk(io.BytesIO(_chunk)) for _chunk in _filtered_chunks]
        filtered = [
            b.title
            for chunk in filtered_chunks
            for b in chunk
        ]
        assert len(eofs) == 1
        assert eofs[0].args[TOTAL] == 4
        assert len(filtered) == 4
        assert b1.title in filtered
        assert b2.title in filtered
        assert b3.title in filtered
        assert b4.title in filtered

    def test_synchronizer_premature_eof(self):
        client_id = uuid.uuid4()
        test_middleware = TestMiddleware()
        serializer = Q1OutSerializer()

        b1, b2, b3, b4 = self.make_4_books()

        # -- -- -- -- Worker 3 does not send books -- -- -- --
        self.append_eof(client_id, test_middleware, 1, len([b1, b2]))
        self.append_chunk(client_id, test_middleware, 1, [b1, b2])
        self.append_chunk(client_id, test_middleware, 2, [b3])
        self.append_eof(client_id, test_middleware, 2, len([b3]))
        self.append_chunk(client_id, test_middleware, 4, [b4])
        self.append_eof(client_id, test_middleware, 4, len([b4]))
        self.append_eof(client_id, test_middleware, 3, len([]))

        worker = Query1Synchronizer(n_workers=4, test_middleware=test_middleware)
        worker.run()

        sent = [Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent]
        eofs = [msg for msg in sent if msg.type == MessageType.EOF]
        _filtered_chunks = [msg.data for msg in sent if msg.type == MessageType.DATA]
        filtered_chunks = [serializer.from_chunk(io.BytesIO(_chunk)) for _chunk in _filtered_chunks]
        filtered = [
            b.title
            for chunk in filtered_chunks
            for b in chunk
        ]
        assert len(eofs) == 1
        assert eofs[0].args[TOTAL] == 4
        assert len(filtered) == 4
        assert b1.title in filtered
        assert b2.title in filtered
        assert b3.title in filtered
        assert b4.title in filtered
        assert test_middleware.callback_counter == 7


if __name__ == '__main__':
    unittest.main()
