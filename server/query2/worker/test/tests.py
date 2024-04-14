import io
import unittest

from utils.q2Partial import Q2Partial
from model.book import Book
from utils.serializer.partialQ2Serializer import PartialQ2Serializer

from utils.protocol import intarr_to_bytes, intarr_from_bytes, TlvTypes, SIZE_LENGTH

class TestUtils(unittest.TestCase):

    def test_intarray(self):
        decades = [1850, 1970, 2020]
        bytes = intarr_to_bytes(decades, TlvTypes.Q2_PARTIAL_DECADES)
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
            authors=['Dennis Ritchie','Brian Kernighan'],
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
            authors=['Dennis Ritchie','Brian Kernighan'],
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
            author = 'Dennis Ritchie',
            decades=[1970, 2000])

        partial1.merge(partial2)

        assert len(partial1.decades) == 3
        assert 1990 in partial1.decades
        assert 2000 in partial1.decades
        assert 1970 in partial1.decades

    def test_partialq2serializer(self):
        serializer = PartialQ2Serializer()

        partial1 = Q2Partial(
            author= 'Dennis Ritchie',
            decades=[1970, 1990, 2000]
        )
        partial2 = Q2Partial(
            author= 'Brian Kernighan',
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

if __name__ == '__main__':
    unittest.main()