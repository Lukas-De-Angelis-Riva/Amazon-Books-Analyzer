import io
import unittest

from model.book import Book
from model.review import Review
from utils.serializer.bookSerializer import BookSerializer
from utils.serializer.reviewSerializer import ReviewSerializer

class TestUtils(unittest.TestCase):
    def test_serializer_can_packet_two_books_in_the_same_mesagge(self):
        serializer = BookSerializer()

        book1 = Book(
            title='At the Montains of madness',
            authors=['H.P. Lovecraft'],
            publisher='Astounding Stories',
            publishedDate='1936',
            categories=['Science fiction', 'Horror'],
        )

        book2 = Book(
            title='The C programming language',
            authors=['Dennis Ritchie','Brian Kernighan'],
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

    def test_serializer_can_packet_two_review_in_the_same_mesagge(self):
        serializer = ReviewSerializer()

        review1 = Review(
            id=1,
            title='At the Montains of madness',
            score=5,
            text="mu' bueno",
        )

        review2 = Review(
            id=2,
            title='The C programming language',
            score=3,
            text="int x80",
        )

        chunk = serializer.to_bytes([review1, review2])
        reader = io.BytesIO(chunk)
        serial = serializer.from_chunk(reader)

        _review1 = serial[0]
        _review2 = serial[1]

        assert review1.id == _review1.id
        assert review1.title == _review1.title
        assert review1.score == _review1.score
        assert review1.text == _review1.text

        assert review2.id == _review2.id
        assert review2.title == _review2.title
        assert review2.score == _review2.score
        assert review2.text == _review2.text

if __name__ == '__main__':
    unittest.main()