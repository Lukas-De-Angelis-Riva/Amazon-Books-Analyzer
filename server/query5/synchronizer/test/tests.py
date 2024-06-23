import unittest
import shutil
import uuid
import os
import io

from common.query5Synchronizer import Query5Synchronizer
from utils.clientTrackerSynchronizer import BASE_DIRECTORY
from utils.worker import TOTAL, WORKER_ID
from utils.middleware.testMiddleware import TestMiddleware
from utils.serializer.q5PartialSerializer import Q5PartialSerializer    # type: ignore
from utils.serializer.q5OutSerializer import Q5OutSerializer            # type: ignore
from dto.q5Partial import Q5Partial
from utils.model.message import Message, MessageType
from utils.model.virus import Disease, virus


class TestUtils(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.path.exists(BASE_DIRECTORY):
            shutil.rmtree(BASE_DIRECTORY)

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
        test_middleware.add_message(eof.to_bytes())

    def append_chunk(self, client_id, test_middleware, peer_id, chunk, chunk_id=None):
        serializer = Q5PartialSerializer()
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
        test_middleware.add_message(msg.to_bytes())

    def make_partials(self):
        n1 = Q5Partial(title='N1', n=10, sentimentAvg=1.0)
        n2 = Q5Partial(title='N2', n=10, sentimentAvg=0.9)
        n3 = Q5Partial(title='N3', n=10, sentimentAvg=0.8)
        n4 = Q5Partial(title='N4', n=10, sentimentAvg=0.7)
        n5 = Q5Partial(title='N5', n=10, sentimentAvg=0.6)
        n6 = Q5Partial(title='N6', n=10, sentimentAvg=0.5)
        n7 = Q5Partial(title='N7', n=10, sentimentAvg=0.4)
        n8 = Q5Partial(title='N8', n=10, sentimentAvg=0.3)
        n9 = Q5Partial(title='N9', n=10, sentimentAvg=0.2)
        n10 = Q5Partial(title='N10', n=10, sentimentAvg=0.1)
        return [n1, n2, n3, n4, n5, n6, n7, n8, n9, n10]

    def check(self, client_id, expected, sent):
        serializer = Q5OutSerializer()
        sent = [msg for msg in sent if msg.client_id == client_id]
        eofs = [msg for msg in sent if msg.type == MessageType.EOF]
        _sent_chunks = [msg.data for msg in sent if msg.type == MessageType.DATA]
        sent_chunks = [serializer.from_chunk(io.BytesIO(_chunk)) for _chunk in _sent_chunks]
        sent_ns = [
            n
            for chunk in sent_chunks
            for n in chunk
        ]
        assert len(eofs) == 1, f'unexpected amount of EOFs, sent: {eofs}'
        assert eofs[0].args[TOTAL] == len(expected), \
            f'wrong EOF[TOTAL] | exp: {len(expected)}, real: {eofs[0].args[TOTAL]}'

        assert len(sent_ns) == len(expected), \
            f'wrong len(sent) | exp: {len(expected)}, real: {len(sent_ns)}'

        for n in expected:
            assert n.title in sent_ns, f'{n.title} not in {sent_ns}'

    def test_sync(self):
        client_id = uuid.UUID('00000000-0000-0000-0000-000000000000')

        test_middleware = TestMiddleware()

        n1, n2, n3, n4, n5, n6, n7, n8, n9, n10 = self.make_partials()
        # -- -- -- -- Worker 1 sends n1, n4, n7 -- -- -- --
        self.append_chunk(client_id, test_middleware, 1, [n1, n4])
        self.append_chunk(client_id, test_middleware, 1, [n7])
        # -- -- -- Worker 2 does not send partials -- -- --
        # -- -- -- -- Worker 3 sends n2, n5, n10, n9 -- -- -- --
        self.append_chunk(client_id, test_middleware, 3, [n10])
        self.append_chunk(client_id, test_middleware, 3, [n9, n2, n5])
        # -- -- -- -- Worker 4 sends n3, n6, n8 -- -- -- --
        self.append_chunk(client_id, test_middleware, 4, [n8])
        self.append_chunk(client_id, test_middleware, 4, [n3])
        self.append_chunk(client_id, test_middleware, 4, [n6])

        self.append_eof(client_id, test_middleware, 1, 3)
        self.append_eof(client_id, test_middleware, 2, 0)
        self.append_eof(client_id, test_middleware, 3, 4)
        self.append_eof(client_id, test_middleware, 4, 3)

        # percentile 0.51 -> n1, n2, n3, n4, n5
        sync = Query5Synchronizer(n_workers=4, chunk_size=2, percentage=51, test_middleware=test_middleware)
        sync.run()

        sent = set([Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent])
        self.check(client_id, [n1, n2, n3, n4, n5], sent)

    def test_sync_premature_eof(self):
        client_id = uuid.UUID('10000000-0000-0000-0000-000000000000')

        test_middleware = TestMiddleware()

        n1, n2, n3, n4, n5, n6, n7, n8, n9, n10 = self.make_partials()

        self.append_eof(client_id, test_middleware, 2, 0)
        self.append_eof(client_id, test_middleware, 3, 4)
        self.append_eof(client_id, test_middleware, 1, 3)
        self.append_eof(client_id, test_middleware, 4, 3)

        # -- -- -- -- Worker 1 sends n1, n4, n7 -- -- -- --
        self.append_chunk(client_id, test_middleware, 1, [n1, n4])
        self.append_chunk(client_id, test_middleware, 1, [n7])
        # -- -- -- Worker 2 does not send partials -- -- --
        # -- -- -- -- Worker 3 sends n2, n5, n10, n9 -- -- -- --
        self.append_chunk(client_id, test_middleware, 3, [n10])
        self.append_chunk(client_id, test_middleware, 3, [n9, n2, n5])
        # -- -- -- -- Worker 4 sends n3, n6, n8 -- -- -- --
        self.append_chunk(client_id, test_middleware, 4, [n8])
        self.append_chunk(client_id, test_middleware, 4, [n3])
        self.append_chunk(client_id, test_middleware, 4, [n6])

        # percentile 0.51 -> n1, n2, n3, n4, n5
        sync = Query5Synchronizer(n_workers=4, chunk_size=2, percentage=51, test_middleware=test_middleware)
        sync.run()

        sent = set([Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent])
        self.check(client_id, [n1, n2, n3, n4, n5], sent)

    def test_sync_sequential_multiclient(self):
        client_1 = uuid.UUID('20000000-0000-0000-0000-000000000000')
        client_2 = uuid.UUID('21000000-0000-0000-0000-000000000000')
        client_3 = uuid.UUID('22000000-0000-0000-0000-000000000000')

        test_middleware = TestMiddleware()

        n1, n2, n3, n4, n5, n6, n7, n8, n9, n10 = self.make_partials()

        # CLIENT 2
        # -- -- -- -- Worker 1 sends n1, n4, n7 -- -- -- --
        self.append_chunk(client_1, test_middleware, 1, [n1, n4])
        self.append_chunk(client_1, test_middleware, 1, [n7])
        # -- -- -- Worker 2 does not send partials -- -- --
        # -- -- -- -- Worker 3 sends n2, n5, n10, n9 -- -- -- --
        self.append_chunk(client_1, test_middleware, 3, [n10])
        self.append_chunk(client_1, test_middleware, 3, [n9, n2, n5])
        # -- -- -- -- Worker 4 sends n3, n6, n8 -- -- -- --
        self.append_chunk(client_1, test_middleware, 4, [n8])
        self.append_chunk(client_1, test_middleware, 4, [n3])
        self.append_chunk(client_1, test_middleware, 4, [n6])

        self.append_eof(client_1, test_middleware, 1, 3)
        self.append_eof(client_1, test_middleware, 2, 0)
        self.append_eof(client_1, test_middleware, 3, 4)
        self.append_eof(client_1, test_middleware, 4, 3)

        # CLIENT 2
        # -- -- -- -- Worker 1 sends n1, n4, n7 -- -- -- --
        self.append_chunk(client_2, test_middleware, 1, [n1, n4, n7])
        # -- -- -- -- Worker 2 sends n2, n5, n10, n9 -- -- -- --
        self.append_chunk(client_2, test_middleware, 2, [n10])
        self.append_chunk(client_2, test_middleware, 2, [n9])
        self.append_chunk(client_2, test_middleware, 2, [n2, n5])
        # -- -- -- Worker 3 does not send partials -- -- --
        # -- -- -- -- Worker 4 sends n3, n6, n8 -- -- -- --
        self.append_chunk(client_2, test_middleware, 4, [n8, n3])
        self.append_chunk(client_2, test_middleware, 4, [n6])

        self.append_eof(client_2, test_middleware, 1, 3)
        self.append_eof(client_2, test_middleware, 2, 4)
        self.append_eof(client_2, test_middleware, 3, 0)
        self.append_eof(client_2, test_middleware, 4, 3)

        # CLIENT 3
        # -- -- -- -- Worker 1 sends n1, n2, n4, n5, n7 -- -- -- --
        self.append_chunk(client_3, test_middleware, 1, [n1, n2, n5, n4])
        self.append_chunk(client_3, test_middleware, 1, [n7])
        # -- -- -- Worker 2 does not send partials -- -- --
        # -- -- -- -- Worker 3 sends n10, n9 -- -- -- --
        self.append_chunk(client_3, test_middleware, 3, [n10])
        self.append_chunk(client_3, test_middleware, 3, [n9])
        # -- -- -- -- Worker 4 sends n3, n6, n8 -- -- -- --
        self.append_chunk(client_3, test_middleware, 4, [n3, n8])
        self.append_chunk(client_3, test_middleware, 4, [n6])

        self.append_eof(client_3, test_middleware, 1, 5)
        self.append_eof(client_3, test_middleware, 2, 0)
        self.append_eof(client_3, test_middleware, 3, 2)
        self.append_eof(client_3, test_middleware, 4, 3)

        # percentile 0.51 -> n1, n2, n3, n4, n5
        sync = Query5Synchronizer(n_workers=4, chunk_size=2, percentage=51, test_middleware=test_middleware)
        sync.run()

        sent = set([Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent])
        self.check(client_1, [n1, n2, n3, n4, n5], sent)
        self.check(client_2, [n1, n2, n3, n4, n5], sent)
        self.check(client_3, [n1, n2, n3, n4, n5], sent)

    def test_sync_parallel_multiclient(self):
        client_1 = uuid.UUID('30000000-0000-0000-0000-000000000000')
        client_2 = uuid.UUID('31000000-0000-0000-0000-000000000000')
        client_3 = uuid.UUID('32000000-0000-0000-0000-000000000000')

        test_middleware = TestMiddleware()

        n1, n2, n3, n4, n5, n6, n7, n8, n9, n10 = self.make_partials()

        # -- -- -- -- --  CHAOS -- -- -- -- --
        self.append_eof(client_3, test_middleware, 1, 5)
        self.append_chunk(client_2, test_middleware, 4, [n8, n3])
        self.append_chunk(client_3, test_middleware, 1, [n7])
        self.append_eof(client_2, test_middleware, 3, 0)
        self.append_eof(client_1, test_middleware, 1, 3)
        self.append_eof(client_1, test_middleware, 2, 0)
        self.append_chunk(client_3, test_middleware, 4, [n3, n8])
        self.append_eof(client_3, test_middleware, 4, 3)
        self.append_chunk(client_1, test_middleware, 1, [n1, n4])
        self.append_chunk(client_1, test_middleware, 4, [n8])
        self.append_eof(client_3, test_middleware, 3, 2)
        self.append_eof(client_2, test_middleware, 1, 3)
        self.append_chunk(client_1, test_middleware, 3, [n9, n2, n5])
        self.append_eof(client_1, test_middleware, 3, 4)
        self.append_chunk(client_3, test_middleware, 1, [n1, n2, n5, n4])
        self.append_chunk(client_1, test_middleware, 4, [n3])
        self.append_chunk(client_3, test_middleware, 4, [n6])
        self.append_chunk(client_1, test_middleware, 1, [n7])
        self.append_eof(client_2, test_middleware, 4, 3)
        self.append_chunk(client_2, test_middleware, 4, [n6])
        self.append_chunk(client_2, test_middleware, 2, [n10])
        self.append_chunk(client_2, test_middleware, 1, [n1, n4, n7])
        self.append_eof(client_3, test_middleware, 2, 0)
        self.append_chunk(client_3, test_middleware, 3, [n9])
        self.append_chunk(client_1, test_middleware, 3, [n10])
        self.append_chunk(client_3, test_middleware, 3, [n10])
        self.append_eof(client_2, test_middleware, 2, 4)
        self.append_chunk(client_2, test_middleware, 2, [n9])
        self.append_chunk(client_2, test_middleware, 2, [n2, n5])
        self.append_eof(client_1, test_middleware, 4, 3)
        self.append_chunk(client_1, test_middleware, 4, [n6])

        # percentile 0.51 -> n1, n2, n3, n4, n5
        sync = Query5Synchronizer(n_workers=4, chunk_size=2, percentage=51, test_middleware=test_middleware)
        sync.run()

        sent = set([Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent])
        self.check(client_1, [n1, n2, n3, n4, n5], sent)
        self.check(client_2, [n1, n2, n3, n4, n5], sent)
        self.check(client_3, [n1, n2, n3, n4, n5], sent)

    def test_infected_sync(self):
        client_id = uuid.UUID('40000000-0000-0000-0000-000000000000')

        test_middleware = TestMiddleware()

        n1, n2, n3, n4, n5, n6, n7, n8, n9, n10 = self.make_partials()
        # -- -- -- -- Worker 1 sends n1, n4, n7 -- -- -- --
        self.append_chunk(client_id, test_middleware, 1, [n1, n4])
        self.append_chunk(client_id, test_middleware, 1, [n7])
        # -- -- -- Worker 2 does not send partials -- -- --
        # -- -- -- -- Worker 3 sends n2, n5, n10, n9 -- -- -- --
        self.append_chunk(client_id, test_middleware, 3, [n10])
        self.append_chunk(client_id, test_middleware, 3, [n9, n2, n5])
        # -- -- -- -- Worker 4 sends n3, n6, n8 -- -- -- --
        self.append_chunk(client_id, test_middleware, 4, [n8])
        self.append_chunk(client_id, test_middleware, 4, [n3])
        self.append_chunk(client_id, test_middleware, 4, [n6])

        self.append_eof(client_id, test_middleware, 1, 3)
        self.append_eof(client_id, test_middleware, 2, 0)
        self.append_eof(client_id, test_middleware, 3, 4)
        self.append_eof(client_id, test_middleware, 4, 3)

        # percentile 0.51 -> n1, n2, n3, n4, n5
        virus.mutate(0.25)
        while True:
            try:
                sync = Query5Synchronizer(n_workers=4, chunk_size=2, percentage=51, test_middleware=test_middleware)
                sync.run()
                break
            except Disease:
                test_middleware.requeue()
                continue
        virus.mutate(0)

        sent = set([Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent])
        self.check(client_id, [n1, n2, n3, n4, n5], sent)

    def test_infected_sync_parallel_multiclient(self):
        client_1 = uuid.UUID('50000000-0000-0000-0000-000000000000')
        client_2 = uuid.UUID('51000000-0000-0000-0000-000000000000')
        client_3 = uuid.UUID('52000000-0000-0000-0000-000000000000')

        test_middleware = TestMiddleware()

        n1, n2, n3, n4, n5, n6, n7, n8, n9, n10 = self.make_partials()

        # -- -- -- -- --  CHAOS -- -- -- -- --
        self.append_eof(client_3, test_middleware, 1, 5)
        self.append_chunk(client_2, test_middleware, 4, [n8, n3])
        self.append_chunk(client_3, test_middleware, 1, [n7])
        self.append_eof(client_2, test_middleware, 3, 0)
        self.append_eof(client_1, test_middleware, 1, 3)
        self.append_eof(client_1, test_middleware, 2, 0)
        self.append_chunk(client_3, test_middleware, 4, [n3, n8])
        self.append_eof(client_3, test_middleware, 4, 3)
        self.append_chunk(client_1, test_middleware, 1, [n1, n4])
        self.append_chunk(client_1, test_middleware, 4, [n8])
        self.append_eof(client_3, test_middleware, 3, 2)
        self.append_eof(client_2, test_middleware, 1, 3)
        self.append_chunk(client_1, test_middleware, 3, [n9, n2, n5])
        self.append_eof(client_1, test_middleware, 3, 4)
        self.append_chunk(client_3, test_middleware, 1, [n1, n2, n5, n4])
        self.append_chunk(client_1, test_middleware, 4, [n3])
        self.append_chunk(client_3, test_middleware, 4, [n6])
        self.append_chunk(client_1, test_middleware, 1, [n7])
        self.append_eof(client_2, test_middleware, 4, 3)
        self.append_chunk(client_2, test_middleware, 4, [n6])
        self.append_chunk(client_2, test_middleware, 2, [n10])
        self.append_chunk(client_2, test_middleware, 1, [n1, n4, n7])
        self.append_eof(client_3, test_middleware, 2, 0)
        self.append_chunk(client_3, test_middleware, 3, [n9])
        self.append_chunk(client_1, test_middleware, 3, [n10])
        self.append_chunk(client_3, test_middleware, 3, [n10])
        self.append_eof(client_2, test_middleware, 2, 4)
        self.append_chunk(client_2, test_middleware, 2, [n9])
        self.append_chunk(client_2, test_middleware, 2, [n2, n5])
        self.append_eof(client_1, test_middleware, 4, 3)
        self.append_chunk(client_1, test_middleware, 4, [n6])

        # percentile 0.51 -> n1, n2, n3, n4, n5
        virus.mutate(0.15)
        while True:
            try:
                sync = Query5Synchronizer(n_workers=4, chunk_size=2, percentage=51, test_middleware=test_middleware)
                sync.run()
                break
            except Disease:
                test_middleware.requeue()
                continue
        virus.mutate(0)

        sent = set([Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent])
        self.check(client_1, [n1, n2, n3, n4, n5], sent)
        self.check(client_2, [n1, n2, n3, n4, n5], sent)
        self.check(client_3, [n1, n2, n3, n4, n5], sent)


if __name__ == '__main__':
    unittest.main()
