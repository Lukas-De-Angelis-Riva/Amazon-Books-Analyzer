import io
import unittest
import uuid

from common.query2Synchronizer import Query2Synchronizer
from utils.worker import TOTAL, WORKER_ID
from utils.middleware.testMiddleware import TestMiddleware
from utils.serializer.q2OutSerializer import Q2OutSerializer    # type: ignore
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
        serializer = Q2OutSerializer()
        msg = Message(
            client_id=client_id,
            type=MessageType.DATA,
            data=serializer.to_bytes(chunk),
            args={
                WORKER_ID: peer_id,
            }
        )
        test_middleware.add_message(msg.to_bytes())

    def test_resultq2serializer(self):
        serializer = Q2OutSerializer()

        result1 = 'Dennis Ritchie'
        result2 = 'Brian Kernighan'
        result3 = 'Andrew S. Tanenbaum'

        chunk = serializer.to_bytes([result1, result2, result3])
        reader = io.BytesIO(chunk)
        serial = serializer.from_chunk(reader)

        _result1 = serial[0]
        _result2 = serial[1]
        _result3 = serial[2]

        assert result1 == _result1
        assert result2 == _result2
        assert result3 == _result3

    def make_authors(self):
        a1 = 'GRR Martin'
        a2 = 'JRR Tolkien'
        a3 = 'JK Rowling'
        a4 = 'Brandon Sanderson'
        a5 = 'Lovecraft'
        a6 = 'Andrzej Sapkowski'
        a7 = 'Stephen King'
        a8 = 'Jane Austen'
        return a1, a2, a3, a4, a5, a6, a7, a8

    def test_sync(self):
        client_id = uuid.uuid4()
        test_middleware = TestMiddleware()
        serializer = Q2OutSerializer()

        a1, a2, a3, a4, a5, a6, a7, a8 = self.make_authors()

        # -- -- -- -- Worker 1 sends a1, a2, a7 & a8 -- -- -- --
        self.append_chunk(client_id, test_middleware, 1, [a1, a2])
        self.append_chunk(client_id, test_middleware, 1, [a7, a8])
        # -- -- -- -- -- Worker 2 sends a5 -- -- -- -- --
        self.append_chunk(client_id, test_middleware, 2, [a5])
        # -- -- -- -- Worker 3 does not send authors -- -- -- --
        # -- -- -- -- Worker 4 sends a3, a4, a6 -- -- -- --
        self.append_chunk(client_id, test_middleware, 4, [a3, a4])
        self.append_chunk(client_id, test_middleware, 4, [a6])

        self.append_eof(client_id, test_middleware, 1, 4)
        self.append_eof(client_id, test_middleware, 2, 1)
        self.append_eof(client_id, test_middleware, 3, 0)
        self.append_eof(client_id, test_middleware, 4, 3)

        sync = Query2Synchronizer(n_workers=4, test_middleware=test_middleware)
        sync.run()

        sent = [Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent]
        eofs = [msg for msg in sent if msg.type == MessageType.EOF]
        _sent_chunks = [msg.data for msg in sent if msg.type == MessageType.DATA]
        sent_chunks = [serializer.from_chunk(io.BytesIO(_chunk)) for _chunk in _sent_chunks]
        sent_authors = [
            a
            for chunk in sent_chunks
            for a in chunk
        ]

        assert len(eofs) == 1
        assert eofs[0].args[TOTAL] == 8

        assert len(sent_authors) == 8
        assert a1 in sent_authors
        assert a2 in sent_authors
        assert a3 in sent_authors
        assert a4 in sent_authors
        assert a5 in sent_authors
        assert a6 in sent_authors
        assert a7 in sent_authors
        assert a8 in sent_authors

    def test_sync_premature_eof(self):
        client_id = uuid.uuid4()
        test_middleware = TestMiddleware()
        serializer = Q2OutSerializer()

        a1, a2, a3, a4, a5, a6, a7, a8 = self.make_authors()

        self.append_eof(client_id, test_middleware, 1, 4)
        self.append_eof(client_id, test_middleware, 3, 0)
        self.append_eof(client_id, test_middleware, 4, 3)
        self.append_eof(client_id, test_middleware, 2, 1)

        # -- -- -- -- Worker 1 sends a1, a2, a7 & a8 -- -- -- --
        self.append_chunk(client_id, test_middleware, 1, [a1, a2])
        self.append_chunk(client_id, test_middleware, 1, [a7, a8])
        # -- -- -- -- -- Worker 2 sends a5 -- -- -- -- --
        self.append_chunk(client_id, test_middleware, 2, [a5])
        # -- -- -- -- Worker 3 does not send authors -- -- -- --
        # -- -- -- -- Worker 4 sends a3, a4, a6 -- -- -- --
        self.append_chunk(client_id, test_middleware, 4, [a3, a4])
        self.append_chunk(client_id, test_middleware, 4, [a6])

        sync = Query2Synchronizer(n_workers=4, test_middleware=test_middleware)
        sync.run()

        sent = [Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent]
        eofs = [msg for msg in sent if msg.type == MessageType.EOF]
        _sent_chunks = [msg.data for msg in sent if msg.type == MessageType.DATA]
        sent_chunks = [serializer.from_chunk(io.BytesIO(_chunk)) for _chunk in _sent_chunks]
        sent_authors = [
            a
            for chunk in sent_chunks
            for a in chunk
        ]

        assert len(eofs) == 1
        assert eofs[0].args[TOTAL] == 8

        assert len(sent_authors) == 8
        assert a1 in sent_authors
        assert a2 in sent_authors
        assert a3 in sent_authors
        assert a4 in sent_authors
        assert a5 in sent_authors
        assert a6 in sent_authors
        assert a7 in sent_authors
        assert a8 in sent_authors

    def test_sync_sequential_multiclient(self):
        client_1 = uuid.uuid4()
        client_2 = uuid.uuid4()
        client_3 = uuid.uuid4()
        test_middleware = TestMiddleware()
        serializer = Q2OutSerializer()

        a1, a2, a3, a4, a5, a6, a7, a8 = self.make_authors()

        # CLIENT 1
        # -- -- -- -- Worker 1 does not send authors -- -- -- --
        # -- -- -- -- -- Worker 2 sends a1 -- -- -- -- --
        self.append_chunk(client_1, test_middleware, 2, [a1])
        # -- -- -- -- Worker 3 does not send authors -- -- -- --
        # -- -- -- -- Worker 4 sends a2, a3 -- -- -- --
        self.append_chunk(client_1, test_middleware, 4, [a2])
        self.append_chunk(client_1, test_middleware, 4, [a3])

        self.append_eof(client_1, test_middleware, 1, 0)
        self.append_eof(client_1, test_middleware, 2, 1)
        self.append_eof(client_1, test_middleware, 3, 0)
        self.append_eof(client_1, test_middleware, 4, 2)

        # CLIENT 2
        # -- -- -- -- -- Worker 1 sends a4 -- -- -- -- --
        self.append_chunk(client_2, test_middleware, 1, [a4])
        # -- -- -- -- Worker 2 does not send authors -- -- -- --
        # -- -- -- -- -- Worker 3 sends a5 -- -- -- -- --
        self.append_chunk(client_2, test_middleware, 3, [a5])
        # -- -- -- -- Worker 4 does not send authors -- -- -- --

        self.append_eof(client_2, test_middleware, 1, 1)
        self.append_eof(client_2, test_middleware, 2, 0)
        self.append_eof(client_2, test_middleware, 3, 1)
        self.append_eof(client_2, test_middleware, 4, 0)

        # CLIENT 3
        # -- -- -- -- -- Worker 1 sends a6 -- -- -- -- --
        self.append_chunk(client_3, test_middleware, 1, [a6])
        # -- -- -- -- -- Worker 2 sends a7 -- -- -- -- --
        self.append_chunk(client_3, test_middleware, 2, [a7])
        # -- -- -- -- -- Worker 3 sends a8 -- -- -- -- --
        self.append_chunk(client_3, test_middleware, 3, [a8])
        # -- -- -- Worker 4 does not send authors -- -- --

        self.append_eof(client_3, test_middleware, 1, 1)
        self.append_eof(client_3, test_middleware, 2, 1)
        self.append_eof(client_3, test_middleware, 3, 1)
        self.append_eof(client_3, test_middleware, 4, 0)

        sync = Query2Synchronizer(n_workers=4, test_middleware=test_middleware)
        sync.run()

        def check(client_id, authors, sent):
            sent = [msg for msg in sent if msg.client_id == client_id]
            eofs = [msg for msg in sent if msg.type == MessageType.EOF]
            _sent_chunks = [msg.data for msg in sent if msg.type == MessageType.DATA]
            sent_chunks = [serializer.from_chunk(io.BytesIO(_chunk)) for _chunk in _sent_chunks]
            sent_authors = [
                a
                for chunk in sent_chunks
                for a in chunk
            ]

            assert len(eofs) == 1
            assert eofs[0].args[TOTAL] == len(authors)

            assert len(sent_authors) == len(authors)
            for a in authors:
                assert a in sent_authors

        sent = [Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent]
        check(client_1, [a1, a2, a3], sent)
        check(client_2, [a4, a5], sent)
        check(client_3, [a6, a7, a8], sent)

    def test_sync_parallel_multiclient(self):
        client_1 = uuid.uuid4()
        client_2 = uuid.uuid4()
        client_3 = uuid.uuid4()
        test_middleware = TestMiddleware()
        serializer = Q2OutSerializer()

        a1, a2, a3, a4, a5, a6, a7, a8 = self.make_authors()

        # -- -- -- -- --  CHAOS -- -- -- -- --
        self.append_eof(client_3, test_middleware, 1, 1)
        self.append_eof(client_3, test_middleware, 4, 0)
        self.append_eof(client_2, test_middleware, 2, 0)
        self.append_chunk(client_1, test_middleware, 4, [a2])
        self.append_eof(client_2, test_middleware, 1, 1)
        self.append_chunk(client_2, test_middleware, 1, [a4])
        self.append_chunk(client_1, test_middleware, 4, [a3])
        self.append_eof(client_1, test_middleware, 4, 2)
        self.append_eof(client_1, test_middleware, 2, 1)
        self.append_eof(client_2, test_middleware, 4, 0)
        self.append_chunk(client_2, test_middleware, 3, [a5])
        self.append_eof(client_3, test_middleware, 2, 1)
        self.append_eof(client_3, test_middleware, 3, 1)
        self.append_chunk(client_3, test_middleware, 3, [a8])
        self.append_eof(client_2, test_middleware, 3, 1)
        self.append_eof(client_1, test_middleware, 1, 0)
        self.append_eof(client_1, test_middleware, 3, 0)
        self.append_chunk(client_3, test_middleware, 1, [a6])
        self.append_chunk(client_3, test_middleware, 2, [a7])
        self.append_chunk(client_1, test_middleware, 2, [a1])

        sync = Query2Synchronizer(n_workers=4, test_middleware=test_middleware)
        sync.run()

        def check(client_id, authors, sent):
            sent = [msg for msg in sent if msg.client_id == client_id]
            eofs = [msg for msg in sent if msg.type == MessageType.EOF]
            _sent_chunks = [msg.data for msg in sent if msg.type == MessageType.DATA]
            sent_chunks = [serializer.from_chunk(io.BytesIO(_chunk)) for _chunk in _sent_chunks]
            sent_authors = [
                a
                for chunk in sent_chunks
                for a in chunk
            ]

            assert len(eofs) == 1
            assert eofs[0].args[TOTAL] == len(authors)

            assert len(sent_authors) == len(authors)
            for a in authors:
                assert a in sent_authors

        sent = [Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent]
        check(client_1, [a1, a2, a3], sent)
        check(client_2, [a4, a5], sent)
        check(client_3, [a6, a7, a8], sent)


if __name__ == '__main__':
    unittest.main()
