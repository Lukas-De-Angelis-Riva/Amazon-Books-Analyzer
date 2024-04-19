from utils.serializer.serializer import Serializer
from utils.protocol import SIZE_LENGTH
from utils.protocol import string_to_bytes, string_from_bytes
from utils.protocol import code_to_bytes
from model.book import Book

class Q1InTypes():
    CHUNK = 0
    BOOK = 1
    BOOK_TITLE = 2
    BOOK_AUTHORS = 3
    BOOK_PUBLISHER = 4
    BOOK_PUBLISHED_DATE = 5
    BOOK_CATEGORIES = 6

class Q1InSerializer(Serializer):
    def make_raw_dict(self):
        return {
            Q1InTypes.BOOK_TITLE : b'',
            Q1InTypes.BOOK_AUTHORS : [],
            Q1InTypes.BOOK_PUBLISHER : b'',
            Q1InTypes.BOOK_PUBLISHED_DATE : b'',
            Q1InTypes.BOOK_CATEGORIES : [],
        }


    def from_raw_dict(self, raw_dict):
        # Verification: all books field should be received
        assert raw_dict[Q1InTypes.BOOK_TITLE], "Invalid book: no title provided"
        assert raw_dict[Q1InTypes.BOOK_AUTHORS], "Invalid book: no authors provided"
        assert raw_dict[Q1InTypes.BOOK_PUBLISHER], "Invalid book: no publisher provided"
        assert raw_dict[Q1InTypes.BOOK_PUBLISHED_DATE], "Invalid book: no date provided"
        assert raw_dict[Q1InTypes.BOOK_CATEGORIES], "Invalid book: no categories provided"

        return Book(
            title = string_from_bytes(raw_dict[Q1InTypes.BOOK_TITLE]),
            authors = [
                string_from_bytes(raw_author) for raw_author in raw_dict[Q1InTypes.BOOK_AUTHORS]
            ],
            publisher = string_from_bytes(raw_dict[Q1InTypes.BOOK_PUBLISHER]),
            publishedDate = string_from_bytes(raw_dict[Q1InTypes.BOOK_PUBLISHED_DATE]),
            categories = [
                string_from_bytes(raw_category) for raw_category in raw_dict[Q1InTypes.BOOK_CATEGORIES]
            ],
        )

    def to_bytes(self, chunk: list):
        raw_chunk = b''

        for book in chunk:
            raw_book = b''
            raw_book += string_to_bytes(book.title, Q1InTypes.BOOK_TITLE)
            for author in book.authors:
                raw_book += string_to_bytes(author, Q1InTypes.BOOK_AUTHORS)
            raw_book += string_to_bytes(book.publisher, Q1InTypes.BOOK_PUBLISHER)
            raw_book += string_to_bytes(book.publishedDate, Q1InTypes.BOOK_PUBLISHED_DATE)
            for category in book.categories:
                raw_book += string_to_bytes(category, Q1InTypes.BOOK_CATEGORIES)

            raw_chunk += code_to_bytes(Q1InTypes.BOOK)
            raw_chunk += int.to_bytes(len(raw_book), SIZE_LENGTH, 'big') 
            raw_chunk += raw_book

        result = code_to_bytes(Q1InTypes.CHUNK)
        result += int.to_bytes(len(chunk), SIZE_LENGTH, 'big') 
        result += raw_chunk

        return result