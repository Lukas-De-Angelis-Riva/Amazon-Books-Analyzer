import io
import unittest

from utils.q3Partial import Q3Partial
from model.review import Review
from utils.serializer.partialQ3Serializer import PartialQ3Serializer


class TestUtils(unittest.TestCase):

    def test_empty_partialq3_update_correctly(self):
        partial = Q3Partial(
            title='The C programming language',
            authors=['Dennis Ritchie','Brian Kernighan'],
            n=0,
            scoreAvg=0.0,
        )
        new_review = Review(
            id=0,
            title='The C programming language',
            score=5.0,
            text='A really good manual'
        )
        partial.update(new_review)

        assert partial.n == 1
        assert abs(partial.scoreAvg - new_review.score) < 1e-4

    def test_partialq3_update_correctly(self):
        partial = Q3Partial(
            title='The C programming language',
            authors=['Dennis Ritchie','Brian Kernighan'],
            n=10,
            scoreAvg=4.5,
        )
        new_review = Review(
            id=0,
            title='The C programming language',
            score=5.0,
            text='A really good manual'
        )
        partial.update(new_review)

        assert partial.n == 11
        assert partial.scoreAvg > 4.5

    def test_partialq3_merge_correctly(self):
        partial1 = Q3Partial(
            title='The C programming language',
            authors=['Dennis Ritchie','Brian Kernighan'],
            n=10,
            scoreAvg=4.5,
        )
        partial2 = Q3Partial(
            title='The C programming language',
            authors=['Dennis Ritchie','Brian Kernighan'],
            n=5,
            scoreAvg=5.0,
        )

        partial1.merge(partial2)

        assert partial1.n == 10+5
        assert abs(partial1.scoreAvg - 14/3) < 1e-4

    def test_partialq2serializer(self):
        serializer = PartialQ3Serializer()

        partial1 = Q3Partial(
            title='The C programming language',
            authors=['Dennis Ritchie','Brian Kernighan'],
            n=10,
            scoreAvg=4.5,
        )
        partial2 = Q3Partial(
            title='The C programming language',
            authors=['Dennis Ritchie','Brian Kernighan'],
            n=5,
            scoreAvg=5.0,
        )

        chunk = serializer.to_bytes([partial1, partial2])
        reader = io.BytesIO(chunk)
        serial = serializer.from_chunk(reader)

        _partial1 = serial[0]
        _partial2 = serial[1]

        assert partial1.title == _partial1.title
        assert partial1.authors == _partial1.authors
        assert partial1.n == _partial1.n
        assert abs(partial1.scoreAvg - _partial1.scoreAvg) < 1e-4

        assert partial2.title == _partial2.title
        assert partial2.authors == _partial2.authors
        assert partial2.n == _partial2.n
        assert abs(partial2.scoreAvg - _partial2.scoreAvg) < 1e-4

if __name__ == '__main__':
    unittest.main()