import io
import unittest
import uuid

from dto.q2Partial import Q2Partial
from model.book import Book
from utils.protocol import intarr_to_bytes, intarr_from_bytes, TlvTypes, SIZE_LENGTH
from utils.middleware.testMiddleware import TestMiddleware
from utils.serializer.q2PartialSerializer import Q2PartialSerializer    # type: ignore
from utils.serializer.q2InSerializer import Q2InSerializer              # type: ignore
from utils.serializer.q2OutSerializer import Q2OutSerializer            # type: ignore
from utils.worker import TOTAL
from utils.model.message import Message, MessageType
from common.query2Worker import Query2Worker


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
        serializer = Q2InSerializer()
        msg = Message(
            client_id=client_id,
            type=MessageType.DATA,
            data=serializer.to_bytes(chunk),
        )
        test_middleware.add_message(msg.to_bytes())

    def test_intarray(self):
        decades = [1850, 1970, 2020]
        bytes = intarr_to_bytes(decades, 1)
        _decades = intarr_from_bytes(bytes[TlvTypes.SIZE_CODE_MSG+SIZE_LENGTH:])
        for decade in decades:
            assert decade in _decades, f"decade {decade} not present"

    def test_empty_partialq2_update_correctly(self):
        partial = Q2Partial(
            author='Dennis Ritchie',
            decades=[]
        )
        new_book = Book(
            title='The C programming language',
            authors=['Dennis Ritchie', 'Brian Kernighan'],
            publisher='Prentice Hall',
            publishedDate='1978',
            categories=['Programming', 'Manual'],
        )
        partial.update(new_book)

        assert len(partial.decades) == 1
        assert 1970 in partial.decades

    def test_partialq2_update_correctly(self):
        partial = Q2Partial(
            author='Dennis Ritchie',
            decades=[1990, 2000]
        )
        new_book = Book(
            title='The C programming language',
            authors=['Dennis Ritchie', 'Brian Kernighan'],
            publisher='Prentice Hall',
            publishedDate='1978',
            categories=['Programming', 'Manual'],
        )
        partial.update(new_book)
        # the old ones must prevail.
        assert 1990 in partial.decades
        assert 2000 in partial.decades
        # the new one is added.
        assert 1970 in partial.decades
        assert len(partial.decades) == 3

    def test_partialq2_merge_correctly(self):
        partial1 = Q2Partial(
            author='Dennis Ritchie',
            decades=[1990, 2000])
        partial2 = Q2Partial(
            author='Dennis Ritchie',
            decades=[1970, 2000])

        partial1.merge(partial2)

        assert len(partial1.decades) == 3
        assert 1990 in partial1.decades
        assert 2000 in partial1.decades
        assert 1970 in partial1.decades

    def test_partialq2serializer(self):
        serializer = Q2PartialSerializer()

        partial1 = Q2Partial(
            author='Dennis Ritchie',
            decades=[1970, 1990, 2000]
        )
        partial2 = Q2Partial(
            author='Brian Kernighan',
            decades=[1970, 1980]
        )

        chunk = serializer.to_bytes([partial1, partial2])
        reader = io.BytesIO(chunk)
        serial = serializer.from_chunk(reader)

        _partial1 = serial[0]
        _partial2 = serial[1]

        assert partial1.author == _partial1.author
        assert partial1.decades == _partial1.decades

        assert partial2.author == _partial2.author
        assert partial2.decades == _partial2.decades

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

    def make_books_mistborn(self):
        tfe = Book(
            title='The Final Empire',
            authors=['Brandon Sanderson'],
            publisher='	Tor Books',
            publishedDate='2006',
            categories=['Juvenile fantasy', 'High fantasy'],
        )
        twoa = Book(
            title='The Well of Ascension',
            authors=['Brandon Sanderson'],
            publisher='	Tor Books',
            publishedDate='2007',
            categories=['Juvenile fantasy', 'High fantasy'],
        )
        thoa = Book(
            title='The Hero Of Ages',
            authors=['Brandon Sanderson'],
            publisher='	Tor Books',
            publishedDate='2008',
            categories=['Juvenile fantasy', 'High fantasy'],
        )
        return tfe, twoa, thoa

    def test_worker_filter(self):
        client_id = uuid.uuid4()
        test_middleware = TestMiddleware()
        out_serializer = Q2OutSerializer()
        b1, b2, b3, b4, b5 = self.make_books_asoiaf()
        c1, c2, c3, c4 = self.make_books_tlotr()
        d1, d2, d3 = self.make_books_mistborn()
        self.append_chunk(client_id, test_middleware, [b1, b2, c1, d1])
        self.append_chunk(client_id, test_middleware, [b3, c2, c3, d2])
        self.append_chunk(client_id, test_middleware, [b4])
        self.append_chunk(client_id, test_middleware, [b5, c4, d3])
        self.append_eof(client_id, test_middleware, 12)

        worker = Query2Worker(peer_id=1, peers=10, chunk_size=2, min_decades=2, test_middleware=test_middleware)
        worker.run()
        sent = [Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent]
        eofs = [msg for msg in sent if msg.type == MessageType.EOF]
        _filtered_chunks = [msg.data for msg in sent if msg.type == MessageType.DATA]
        filtered_chunks = [out_serializer.from_chunk(io.BytesIO(_chunk)) for _chunk in _filtered_chunks]
        filtered = [
            a
            for chunk in filtered_chunks
            for a in chunk
        ]

        assert len(eofs) == 1
        assert eofs[0].args[TOTAL] == 2
        assert len(filtered) == 2
        assert b1.authors[0] in filtered
        assert c1.authors[0] in filtered
        assert d1.authors[0] not in filtered

    def test_worker_premature_eof(self):
        client_id = uuid.uuid4()
        test_middleware = TestMiddleware()
        out_serializer = Q2OutSerializer()
        b1, b2, b3, b4, b5 = self.make_books_asoiaf()
        c1, c2, c3, c4 = self.make_books_tlotr()
        d1, d2, d3 = self.make_books_mistborn()
        self.append_eof(client_id, test_middleware, 12)
        self.append_chunk(client_id, test_middleware, [b1, b2, c1, d1])
        self.append_chunk(client_id, test_middleware, [b3, c2, c3, d2])
        self.append_chunk(client_id, test_middleware, [b4])
        self.append_chunk(client_id, test_middleware, [b5, c4, d3])

        worker = Query2Worker(peer_id=1, peers=10, chunk_size=2, min_decades=2, test_middleware=test_middleware)
        worker.run()
        sent = [Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent]
        eofs = [msg for msg in sent if msg.type == MessageType.EOF]
        _filtered_chunks = [msg.data for msg in sent if msg.type == MessageType.DATA]
        filtered_chunks = [out_serializer.from_chunk(io.BytesIO(_chunk)) for _chunk in _filtered_chunks]
        filtered = [
            a
            for chunk in filtered_chunks
            for a in chunk
        ]

        assert len(eofs) == 1
        assert eofs[0].args[TOTAL] == 2
        assert len(filtered) == 2
        assert b1.authors[0] in filtered
        assert c1.authors[0] in filtered
        assert d1.authors[0] not in filtered

    def test_sequential_multiclient(self):
        client_1 = uuid.uuid4()
        client_2 = uuid.uuid4()
        test_middleware = TestMiddleware()
        out_serializer = Q2OutSerializer()
        b1, b2, b3, b4, b5 = self.make_books_asoiaf()
        c1, c2, c3, c4 = self.make_books_tlotr()
        d1, d2, d3 = self.make_books_mistborn()

        self.append_chunk(client_1, test_middleware, [b1, b2, d1])
        self.append_chunk(client_1, test_middleware, [b3, b4, d2])
        self.append_chunk(client_1, test_middleware, [b5, d3])
        self.append_eof(client_1, test_middleware, 8)

        self.append_chunk(client_2, test_middleware, [c1, c2, d1])
        self.append_chunk(client_2, test_middleware, [c3, d2])
        self.append_chunk(client_2, test_middleware, [c4, d3])
        self.append_eof(client_2, test_middleware, 7)

        worker = Query2Worker(peer_id=1, peers=10, chunk_size=2, min_decades=2, test_middleware=test_middleware)
        worker.run()
        sent = [Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent]

        sent_1 = [msg for msg in sent if msg.client_id == client_1]
        eofs = [msg for msg in sent_1 if msg.type == MessageType.EOF]
        _filtered_chunks = [msg.data for msg in sent_1 if msg.type == MessageType.DATA]
        filtered_chunks = [out_serializer.from_chunk(io.BytesIO(_chunk)) for _chunk in _filtered_chunks]
        filtered = [
            a
            for chunk in filtered_chunks
            for a in chunk
        ]
        assert len(eofs) == 1
        assert eofs[0].args[TOTAL] == 1
        assert len(filtered) == 1
        assert b1.authors[0] in filtered
        assert d1.authors[0] not in filtered

        sent_2 = [msg for msg in sent if msg.client_id == client_2]
        eofs = [msg for msg in sent_2 if msg.type == MessageType.EOF]
        _filtered_chunks = [msg.data for msg in sent_2 if msg.type == MessageType.DATA]
        filtered_chunks = [out_serializer.from_chunk(io.BytesIO(_chunk)) for _chunk in _filtered_chunks]
        filtered = [
            a
            for chunk in filtered_chunks
            for a in chunk
        ]
        assert len(eofs) == 1
        assert eofs[0].args[TOTAL] == 1
        assert len(filtered) == 1
        assert c1.authors[0] in filtered
        assert d1.authors[0] not in filtered

    def test_parallel_multiclient(self):
        client_1 = uuid.uuid4()
        client_2 = uuid.uuid4()
        test_middleware = TestMiddleware()
        out_serializer = Q2OutSerializer()
        b1, b2, b3, b4, b5 = self.make_books_asoiaf()
        c1, c2, c3, c4 = self.make_books_tlotr()
        d1, d2, d3 = self.make_books_mistborn()

        self.append_chunk(client_1, test_middleware, [b1, b2, d1])
        self.append_eof(client_1, test_middleware, 8)
        self.append_chunk(client_2, test_middleware, [c1, c2, d1])
        self.append_chunk(client_1, test_middleware, [b3, b4, d2])
        self.append_chunk(client_2, test_middleware, [c3, d2])
        self.append_eof(client_2, test_middleware, 7)
        self.append_chunk(client_1, test_middleware, [b5, d3])
        self.append_chunk(client_2, test_middleware, [c4, d3])

        worker = Query2Worker(peer_id=1, peers=10, chunk_size=2, min_decades=2, test_middleware=test_middleware)
        worker.run()
        sent = [Message.from_bytes(raw_msg) for raw_msg in test_middleware.sent]

        sent_1 = [msg for msg in sent if msg.client_id == client_1]
        eofs = [msg for msg in sent_1 if msg.type == MessageType.EOF]
        _filtered_chunks = [msg.data for msg in sent_1 if msg.type == MessageType.DATA]
        filtered_chunks = [out_serializer.from_chunk(io.BytesIO(_chunk)) for _chunk in _filtered_chunks]
        filtered = [
            a
            for chunk in filtered_chunks
            for a in chunk
        ]
        assert len(eofs) == 1
        assert eofs[0].args[TOTAL] == 1
        assert len(filtered) == 1
        assert b1.authors[0] in filtered
        assert d1.authors[0] not in filtered

        sent_2 = [msg for msg in sent if msg.client_id == client_2]
        eofs = [msg for msg in sent_2 if msg.type == MessageType.EOF]
        _filtered_chunks = [msg.data for msg in sent_2 if msg.type == MessageType.DATA]
        filtered_chunks = [out_serializer.from_chunk(io.BytesIO(_chunk)) for _chunk in _filtered_chunks]
        filtered = [
            a
            for chunk in filtered_chunks
            for a in chunk
        ]
        assert len(eofs) == 1
        assert eofs[0].args[TOTAL] == 1
        assert len(filtered) == 1
        assert c1.authors[0] in filtered
        assert d1.authors[0] not in filtered


if __name__ == '__main__':
    unittest.main()
