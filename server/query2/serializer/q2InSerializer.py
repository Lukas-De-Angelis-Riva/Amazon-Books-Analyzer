from utils.serializer.serializer import Serializer
from utils.protocol import SIZE_LENGTH
from utils.protocol import string_to_bytes, string_from_bytes
from utils.protocol import code_to_bytes
from model.book import Book

class Q2InTypes():
    CHUNK = 0
    BOOK = 1
    BOOK_AUTHORS = 2
    BOOK_PUBLISHED_DATE = 3

class Q2InSerializer(Serializer):
    def make_raw_dict(self):
        return {
            Q2InTypes.BOOK_AUTHORS : [],
            Q2InTypes.BOOK_PUBLISHED_DATE : b'',
        }


    def from_raw_dict(self, raw_dict):
        # Verification: all books field should be received
        assert raw_dict[Q2InTypes.BOOK_AUTHORS], "Invalid book: no authors provided"
        assert raw_dict[Q2InTypes.BOOK_PUBLISHED_DATE], "Invalid book: no date provided"

        return Book(
            title = "",
            authors = [
                string_from_bytes(raw_author) for raw_author in raw_dict[Q2InTypes.BOOK_AUTHORS]
            ],
            publisher = "",
            publishedDate = string_from_bytes(raw_dict[Q2InTypes.BOOK_PUBLISHED_DATE]),
            categories = [],
        )

    def to_bytes(self, chunk: list):
        raw_chunk = b''

        for book in chunk:
            raw_book = b''
            for author in book.authors:
                raw_book += string_to_bytes(author, Q2InTypes.BOOK_AUTHORS)
            raw_book += string_to_bytes(book.publishedDate, Q2InTypes.BOOK_PUBLISHED_DATE)

            raw_chunk += code_to_bytes(Q2InTypes.BOOK)
            raw_chunk += int.to_bytes(len(raw_book), SIZE_LENGTH, 'big') 
            raw_chunk += raw_book

        result = code_to_bytes(Q2InTypes.CHUNK)
        result += int.to_bytes(len(chunk), SIZE_LENGTH, 'big') 
        result += raw_chunk

        return result