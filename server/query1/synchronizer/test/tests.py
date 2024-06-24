import unittest
import shutil
import uuid
import os
import io

from model.book import Book
from common.query1Synchronizer import Query1Synchronizer, IN_QUEUE_NAME
from utils.clientTrackerSynchronizer import BASE_DIRECTORY
from utils.worker import TOTAL, WORKER_ID
from utils.middleware.testMiddleware import TestMiddleware
from utils.serializer.q1OutSerializer import Q1OutSerializer    # type: ignore
from utils.model.message import Message, MessageType
from utils.model.virus import Disease, virus


class TestUtils(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.path.exists(BASE_DIRECTORY):
            shutil.rmtree(BASE_DIRECTORY)

    def setUp(self):
        virus.regenerate()

    def append_eof(self, client_id, test_middleware, peer_id, sent, eof_id=None):
        eof = Message(
            client_id=client_id,
            type=MessageType.EOF,
            data=b'',
            args={
                TOTAL: sent,
                WORKER_ID: peer_id,
            }
        )
        if eof_id:
            eof.ID = eof_id
        test_middleware.add_message(eof.to_bytes(), IN_QUEUE_NAME)

    def append_chunk(self, client_id, test_middleware, peer_id, chunk, chunk_id=None):
        serializer = Q1OutSerializer()
        msg = Message(
            client_id=client_id,
            type=MessageType.DATA,
            data=serializer.to_bytes(chunk),
            args={
                WORKER_ID: peer_id,
            }
        )
        if chunk_id:
            msg.ID = chunk_id
        test_middleware.add_message(msg.to_bytes(), IN_QUEUE_NAME)

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

    def test_sync(self):
        client_id = uuid.UUID('00000000-0000-0000-0000-000000000000')
        test_middleware = TestMiddleware()

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

        sent = set([Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent])
        self.check(client_id, [b1, b2, b3, b4], sent)

    def test_synchronizer_premature_eof(self):
        client_id = uuid.UUID('10000000-0000-0000-0000-000000000000')
        test_middleware = TestMiddleware()

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

        sent = set([Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent])
        self.check(client_id, [b1, b2, b3, b4], sent)

    def test_sync_sequential_multiclient(self):
        client_1 = uuid.UUID('20000000-0000-0000-0000-000000000000')
        client_2 = uuid.UUID('21000000-0000-0000-0000-000000000000')
        client_3 = uuid.UUID('22000000-0000-0000-0000-000000000000')
        test_middleware = TestMiddleware()

        b1, b2, b3, b4 = self.make_4_books()

        # CLIENT 1
        self.append_chunk(client_1, test_middleware, 1, [b1, b2])
        self.append_chunk(client_1, test_middleware, 2, [b3])
        # -- -- -- -- Worker 3 does not send books -- -- -- --
        self.append_chunk(client_1, test_middleware, 4, [b4])

        self.append_eof(client_1, test_middleware, 1, len([b1, b2]))
        self.append_eof(client_1, test_middleware, 2, len([b3]))
        self.append_eof(client_1, test_middleware, 3, len([]))
        self.append_eof(client_1, test_middleware, 4, len([b4]))

        # CLIENT 2
        self.append_chunk(client_2, test_middleware, 1, [b1])
        self.append_chunk(client_2, test_middleware, 2, [b3])
        self.append_chunk(client_2, test_middleware, 3, [b2])
        self.append_chunk(client_2, test_middleware, 4, [b4])

        self.append_eof(client_2, test_middleware, 1, len([b1]))
        self.append_eof(client_2, test_middleware, 2, len([b3]))
        self.append_eof(client_2, test_middleware, 3, len([b2]))
        self.append_eof(client_2, test_middleware, 4, len([b4]))

        # CLIENT 3
        # -- -- -- -- Worker 1 does not send books -- -- -- --
        self.append_chunk(client_3, test_middleware, 2, [b3, b1, b4])
        # -- -- -- -- Worker 3 does not send books -- -- -- --
        self.append_chunk(client_3, test_middleware, 4, [b2])

        self.append_eof(client_3, test_middleware, 1, len([]))
        self.append_eof(client_3, test_middleware, 2, len([b3, b1, b4]))
        self.append_eof(client_3, test_middleware, 3, len([]))
        self.append_eof(client_3, test_middleware, 4, len([b2]))

        worker = Query1Synchronizer(n_workers=4, test_middleware=test_middleware)
        worker.run()

        sent = set([Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent])
        self.check(client_1, [b1, b2, b3, b4], sent)
        self.check(client_2, [b1, b2, b3, b4], sent)
        self.check(client_3, [b1, b2, b3, b4], sent)

    def test_sync_parallel_multiclient(self):
        client_1 = uuid.UUID('30000000-0000-0000-0000-000000000000')
        client_2 = uuid.UUID('31000000-0000-0000-0000-000000000000')
        client_3 = uuid.UUID('32000000-0000-0000-0000-000000000000')
        test_middleware = TestMiddleware()

        b1, b2, b3, b4 = self.make_4_books()

        # -- -- -- -- --  CHAOS -- -- -- -- --
        self.append_eof(client_2, test_middleware, 4, len([b4]))
        self.append_eof(client_1, test_middleware, 1, len([b1, b2]))
        self.append_eof(client_1, test_middleware, 3, len([]))
        self.append_chunk(client_2, test_middleware, 4, [b4])
        self.append_chunk(client_1, test_middleware, 1, [b1, b2])
        self.append_eof(client_1, test_middleware, 4, len([b4]))
        self.append_chunk(client_3, test_middleware, 2, [b3, b1, b4])
        self.append_eof(client_2, test_middleware, 3, len([b2]))
        self.append_chunk(client_2, test_middleware, 1, [b1])
        self.append_eof(client_3, test_middleware, 4, len([b2]))
        self.append_eof(client_1, test_middleware, 2, len([b3]))
        self.append_eof(client_3, test_middleware, 1, len([]))
        self.append_chunk(client_3, test_middleware, 4, [b2])
        self.append_chunk(client_1, test_middleware, 2, [b3])
        self.append_eof(client_3, test_middleware, 2, len([b3, b1, b4]))
        self.append_chunk(client_2, test_middleware, 2, [b3])
        self.append_chunk(client_1, test_middleware, 4, [b4])
        self.append_chunk(client_2, test_middleware, 3, [b2])
        self.append_eof(client_3, test_middleware, 3, len([]))
        self.append_eof(client_2, test_middleware, 1, len([b1]))
        self.append_eof(client_2, test_middleware, 2, len([b3]))

        worker = Query1Synchronizer(n_workers=4, test_middleware=test_middleware)
        worker.run()

        sent = set([Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent])
        self.check(client_1, [b1, b2, b3, b4], sent)
        self.check(client_2, [b1, b2, b3, b4], sent)
        self.check(client_3, [b1, b2, b3, b4], sent)

    def test_infected_sync(self):
        client_id = uuid.UUID('40000000-0000-0000-0000-000000000000')
        test_middleware = TestMiddleware()

        b1, b2, b3, b4 = self.make_4_books()
        self.append_chunk(client_id, test_middleware, 1, [b1, b2])
        self.append_chunk(client_id, test_middleware, 2, [b3])
        # -- -- -- -- Worker 3 does not send books -- -- -- --
        self.append_chunk(client_id, test_middleware, 4, [b4])

        self.append_eof(client_id, test_middleware, 1, len([b1, b2]))
        self.append_eof(client_id, test_middleware, 2, len([b3]))
        self.append_eof(client_id, test_middleware, 3, len([]))
        self.append_eof(client_id, test_middleware, 4, len([b4]))

        virus.mutate(0.25)
        while True:
            try:
                worker = Query1Synchronizer(n_workers=4, test_middleware=test_middleware)
                worker.run()
                break
            except Disease:
                test_middleware.requeue()
                continue

        sent = set([Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent])
        self.check(client_id, [b1, b2, b3, b4], sent)

    def test_infected_sync_parallel_multiclient(self):
        client_1 = uuid.UUID('50000000-0000-0000-0000-000000000000')
        client_2 = uuid.UUID('51000000-0000-0000-0000-000000000000')
        client_3 = uuid.UUID('52000000-0000-0000-0000-000000000000')
        test_middleware = TestMiddleware()

        b1, b2, b3, b4 = self.make_4_books()

        # -- -- -- -- --  CHAOS -- -- -- -- --
        self.append_eof(client_2, test_middleware, 4, len([b4]))
        self.append_eof(client_1, test_middleware, 1, len([b1, b2]))
        self.append_eof(client_1, test_middleware, 3, len([]))
        self.append_chunk(client_2, test_middleware, 4, [b4])
        self.append_chunk(client_1, test_middleware, 1, [b1, b2])
        self.append_eof(client_1, test_middleware, 4, len([b4]))
        self.append_chunk(client_3, test_middleware, 2, [b3, b1, b4])
        self.append_eof(client_2, test_middleware, 3, len([b2]))
        self.append_chunk(client_2, test_middleware, 1, [b1])
        self.append_eof(client_3, test_middleware, 4, len([b2]))
        self.append_eof(client_1, test_middleware, 2, len([b3]))
        self.append_eof(client_3, test_middleware, 1, len([]))
        self.append_chunk(client_3, test_middleware, 4, [b2])
        self.append_chunk(client_1, test_middleware, 2, [b3])
        self.append_eof(client_3, test_middleware, 2, len([b3, b1, b4]))
        self.append_chunk(client_2, test_middleware, 2, [b3])
        self.append_chunk(client_1, test_middleware, 4, [b4])
        self.append_chunk(client_2, test_middleware, 3, [b2])
        self.append_eof(client_3, test_middleware, 3, len([]))
        self.append_eof(client_2, test_middleware, 1, len([b1]))
        self.append_eof(client_2, test_middleware, 2, len([b3]))

        virus.mutate(0.15)
        while True:
            try:
                worker = Query1Synchronizer(n_workers=4, test_middleware=test_middleware)
                worker.run()
                break
            except Disease:
                test_middleware.requeue()
                continue

        sent = set([Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent])
        self.check(client_1, [b1, b2, b3, b4], sent)
        self.check(client_2, [b1, b2, b3, b4], sent)
        self.check(client_3, [b1, b2, b3, b4], sent)


if __name__ == '__main__':
    unittest.main()
