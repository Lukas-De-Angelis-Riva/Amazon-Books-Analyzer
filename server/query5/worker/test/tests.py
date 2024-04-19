import io
import unittest

from dto.q5Partial import Q5Partial
from model.review import Review
from utils.serializer.q5PartialSerializer import Q5PartialSerializer 

class TestUtils(unittest.TestCase):
    def test_empty_partialq3_update_correctly(self):
        partial = Q5Partial(
            title='The C programming language',
            n=0,
            sentimentAvg=0.0,
        )
        new_review = Review(
            id=0,
            title='The C programming language',
            score=5.0,
            text='A really good manual'
        )
        partial.update(new_review)

        assert partial.n == 1
        assert partial.sentimentAvg > 0

    def test_partialq3_update_correctly(self):
        partial = Q5Partial(
            title='The C programming language',
            n=10,
            sentimentAvg=0.0,
        )
        new_review = Review(
            id=0,
            title='The C programming language',
            score=5.0,
            text='A really good manual'
        )
        partial.update(new_review)

        assert partial.n == 11
        assert partial.sentimentAvg > 0

    def test_sentiment_function(self):
        partial1 = Q5Partial(
            title='The C programming language',
            n=0,
            sentimentAvg=0.0,
        )
        review1 = Review(
            id=0,
            title='The C programming language',
            score=5.0,
            text='A really good manual' # positive text
        )
        partial1.update(review1)

        partial2 = Q5Partial(
            title='The C programming language',
            n=0,
            sentimentAvg=0.0,
        )
        review2 = Review(
            id=0,
            title='The C programming language',
            score=5.0,
            text='This manual is useless' # negative text
        )
        partial2.update(review2)
        
        assert partial1.sentimentAvg > partial2.sentimentAvg

    def test_partialq3_merge_correctly(self):
        partial1 = Q5Partial(
            title='The C programming language',
            n=10,
            sentimentAvg=4.5,
        )
        partial2 = Q5Partial(
            title='The C programming language',
            n=5,
            sentimentAvg=5.0,
        )

        partial1.merge(partial2)

        assert partial1.n == 10+5
        assert abs(partial1.sentimentAvg - 14/3) < 1e-4

    def test_partialq3serializer(self):
        serializer = Q5PartialSerializer()

        partial1 = Q5Partial(
            title='The C programming language',
            n=10,
            sentimentAvg=4.5,
        )
        partial2 = Q5Partial(
            title='The C programming language',
            n=5,
            sentimentAvg=5.0,
        )

        chunk = serializer.to_bytes([partial1, partial2])
        reader = io.BytesIO(chunk)
        serial = serializer.from_chunk(reader)

        _partial1 = serial[0]
        _partial2 = serial[1]

        assert partial1.title == _partial1.title
        assert partial1.n == _partial1.n
        assert abs(partial1.sentimentAvg - _partial1.sentimentAvg) < 1e-4

        assert partial2.title == _partial2.title
        assert partial2.n == _partial2.n
        assert abs(partial2.sentimentAvg - _partial2.sentimentAvg) < 1e-4

if __name__ == '__main__':
    unittest.main()