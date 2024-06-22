import unittest
import shutil
import uuid
import os
import io

from model.book import Book
from common.query1Synchronizer import Query1Synchronizer
from utils.clientTrackerSynchronizer import BASE_DIRECTORY
from utils.worker import TOTAL, WORKER_ID
from utils.middleware.testMiddleware import TestMiddleware
from utils.serializer.q1OutSerializer import Q1OutSerializer        # type: ignore
from utils.model.message import Message, MessageType
from utils.model.virus import Disease, virus


class TestUtils(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.path.exists(BASE_DIRECTORY):
            shutil.rmtree(BASE_DIRECTORY)

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

    def test_sync(self):
        client_id = uuid.UUID('00000000-0000-0000-0000-000000000000')
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

        sent = set([Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent])
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
        client_id = uuid.UUID('10000000-0000-0000-0000-000000000000')
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

        sent = set([Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent])
        eofs = [msg for msg in sent if msg.type == MessageType.EOF]
        _filtered_chunks = [msg.data for msg in sent if msg.type == MessageType.DATA]
        filtered_chunks = [serializer.from_chunk(io.BytesIO(_chunk)) for _chunk in _filtered_chunks]
        filtered = [
            b.title
            for chunk in filtered_chunks
            for b in chunk
        ]
        assert len(eofs) == 1, f'unexpected amount of EOF, sent: {eofs}'
        assert eofs[0].args[TOTAL] == 4, f'wrong EOF[TOTAL] | exp: {4}, real: {eofs[0].args[TOTAL]}'
        assert len(filtered) == 4, f'wrong len(filtered) | exp: {4}, real: {len(filtered)}'
        assert b1.title in filtered, f'{b1.title} not in {filtered}'
        assert b2.title in filtered, f'{b2.title} not in {filtered}'
        assert b3.title in filtered, f'{b3.title} not in {filtered}'
        assert b4.title in filtered, f'{b4.title} not in {filtered}'
        assert test_middleware.callback_counter == 7

    def test_sync_sequential_multiclient(self):
        client_1 = uuid.UUID('20000000-0000-0000-0000-000000000000')
        client_2 = uuid.UUID('21000000-0000-0000-0000-000000000000')
        client_3 = uuid.UUID('22000000-0000-0000-0000-000000000000')
        test_middleware = TestMiddleware()
        serializer = Q1OutSerializer()

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

        def check(client_id, books, sent):
            sent = [msg for msg in sent if msg.client_id == client_id]
            eofs = [msg for msg in sent if msg.type == MessageType.EOF]
            _filtered_chunks = [msg.data for msg in sent if msg.type == MessageType.DATA]
            filtered_chunks = [serializer.from_chunk(io.BytesIO(_chunk)) for _chunk in _filtered_chunks]
            filtered = [
                b.title
                for chunk in filtered_chunks
                for b in chunk
            ]
            assert len(eofs) == 1, f'unexpected amount of EOF, sent: {eofs}'
            assert eofs[0].args[TOTAL] == len(books), \
                f'wrong EOF[TOTAL] | exp: {len(books)}, real: {eofs[0].args[TOTAL]}'

            assert len(filtered) == len(books), \
                f'wrong len(sent) | exp: {len(books)}, real: {len(filtered)}'

            for b in books:
                assert b.title in filtered, f'{b.title} not in {filtered}'

        sent = set([Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent])
        check(client_1, [b1, b2, b3, b4], sent)
        check(client_2, [b1, b2, b3, b4], sent)
        check(client_3, [b1, b2, b3, b4], sent)

    def test_sync_parallel_multiclient(self):
        client_1 = uuid.UUID('30000000-0000-0000-0000-000000000000')
        client_2 = uuid.UUID('31000000-0000-0000-0000-000000000000')
        client_3 = uuid.UUID('32000000-0000-0000-0000-000000000000')
        test_middleware = TestMiddleware()
        serializer = Q1OutSerializer()

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

        def check(client_id, books, sent):
            sent = [msg for msg in sent if msg.client_id == client_id]
            eofs = [msg for msg in sent if msg.type == MessageType.EOF]
            _filtered_chunks = [msg.data for msg in sent if msg.type == MessageType.DATA]
            filtered_chunks = [serializer.from_chunk(io.BytesIO(_chunk)) for _chunk in _filtered_chunks]
            filtered = [
                b.title
                for chunk in filtered_chunks
                for b in chunk
            ]
            assert len(eofs) == 1, f'unexpected amount of EOF, sent: {eofs}'
            assert eofs[0].args[TOTAL] == len(books), \
                f'wrong EOF[TOTAL] | exp: {len(books)}, real: {eofs[0].args[TOTAL]}'

            assert len(filtered) == len(books), \
                f'wrong len(sent) | exp: {len(books)}, real: {len(filtered)}'

            for b in books:
                assert b.title in filtered, f'{b.title} not in {filtered}'

        sent = set([Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent])
        check(client_1, [b1, b2, b3, b4], sent)
        check(client_2, [b1, b2, b3, b4], sent)
        check(client_3, [b1, b2, b3, b4], sent)

    def test_infected_sync(self):
        client_id = uuid.UUID('40000000-0000-0000-0000-000000000000')
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

        virus.mutate(0.25)
        while True:
            try:
                worker = Query1Synchronizer(n_workers=4, test_middleware=test_middleware)
                worker.run()
                break
            except Disease:
                continue
        virus.mutate(0)

        sent = set([Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent])
        eofs = [msg for msg in sent if msg.type == MessageType.EOF]
        _filtered_chunks = [msg.data for msg in sent if msg.type == MessageType.DATA]
        filtered_chunks = [serializer.from_chunk(io.BytesIO(_chunk)) for _chunk in _filtered_chunks]
        filtered = [
            b.title
            for chunk in filtered_chunks
            for b in chunk
        ]
        assert len(eofs) == 1, f'unexpected amount of EOF, sent: {eofs}'
        assert eofs[0].args[TOTAL] == 4, f'wrong EOF[TOTAL] | exp: {4}, real: {eofs[0].args[TOTAL]}'
        assert len(filtered) == 4, f'wrong len(filtered) | exp: {4}, real: {len(filtered)}'
        assert b1.title in filtered, f'{b1.title} not in {filtered}'
        assert b2.title in filtered, f'{b2.title} not in {filtered}'
        assert b3.title in filtered, f'{b3.title} not in {filtered}'
        assert b4.title in filtered, f'{b4.title} not in {filtered}'

    def test_infected_sync_parallel_multiclient(self):
        client_1 = uuid.UUID('50000000-0000-0000-0000-000000000000')
        client_2 = uuid.UUID('51000000-0000-0000-0000-000000000000')
        client_3 = uuid.UUID('52000000-0000-0000-0000-000000000000')
        test_middleware = TestMiddleware()
        serializer = Q1OutSerializer()

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
                continue
        virus.mutate(0)

        def check(client_id, books, sent):
            sent = [msg for msg in sent if msg.client_id == client_id]
            eofs = [msg for msg in sent if msg.type == MessageType.EOF]
            _filtered_chunks = [msg.data for msg in sent if msg.type == MessageType.DATA]
            filtered_chunks = [serializer.from_chunk(io.BytesIO(_chunk)) for _chunk in _filtered_chunks]
            filtered = [
                b.title
                for chunk in filtered_chunks
                for b in chunk
            ]
            assert len(eofs) == 1, f'unexpected amount of EOF, sent: {eofs}'
            assert eofs[0].args[TOTAL] == len(books), \
                f'wrong EOF[TOTAL] | exp: {len(books)}, real: {eofs[0].args[TOTAL]}'

            assert len(filtered) == len(books), \
                f'wrong len(sent) | exp: {len(books)}, real: {len(filtered)}'

            for b in books:
                assert b.title in filtered, f'{b.title} not in {filtered}'

        sent = set([Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent])
        check(client_1, [b1, b2, b3, b4], sent)
        check(client_2, [b1, b2, b3, b4], sent)
        check(client_3, [b1, b2, b3, b4], sent)


if __name__ == '__main__':
    unittest.main()
