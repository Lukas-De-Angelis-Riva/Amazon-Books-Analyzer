import io
import unittest

from model.book import Book
from utils.serializer.bookSerializer import BookSerializer

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

if __name__ == '__main__':
    unittest.main()