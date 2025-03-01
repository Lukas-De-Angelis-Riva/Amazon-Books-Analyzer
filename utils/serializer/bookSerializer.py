from utils.serializer.serializer import Serializer
from utils.protocol import TlvTypes, SIZE_LENGTH
from utils.protocol import string_to_bytes, string_from_bytes
from utils.protocol import code_to_bytes
from model.book import Book


class BookSerializer(Serializer):
    def make_raw_dict(self):
        return {
            TlvTypes.BOOK_TITLE: b'',
            TlvTypes.BOOK_AUTHORS: [],
            TlvTypes.BOOK_PUBLISHER: b'',
            TlvTypes.BOOK_PUBLISHED_DATE: b'',
            TlvTypes.BOOK_CATEGORIES: [],
        }

    def from_raw_dict(self, raw_dict):
        # Verification: all books field should be received
        assert raw_dict[TlvTypes.BOOK_TITLE], "Invalid book: no title provided"
        assert raw_dict[TlvTypes.BOOK_AUTHORS], "Invalid book: no authors provided"
        assert raw_dict[TlvTypes.BOOK_PUBLISHER], "Invalid book: no publisher provided"
        assert raw_dict[TlvTypes.BOOK_PUBLISHED_DATE], "Invalid book: no date provided"
        assert raw_dict[TlvTypes.BOOK_CATEGORIES], "Invalid book: no categories provided"

        return Book(
            title=string_from_bytes(raw_dict[TlvTypes.BOOK_TITLE]),
            authors=[
                string_from_bytes(raw_author) for raw_author in raw_dict[TlvTypes.BOOK_AUTHORS]
            ],
            publisher=string_from_bytes(raw_dict[TlvTypes.BOOK_PUBLISHER]),
            publishedDate=string_from_bytes(raw_dict[TlvTypes.BOOK_PUBLISHED_DATE]),
            categories=[
                string_from_bytes(raw_category) for raw_category in raw_dict[TlvTypes.BOOK_CATEGORIES]
            ],
        )

    def to_bytes(self, chunk: list):
        raw_chunk = b''

        for book in chunk:
            raw_book = b''
            raw_book += string_to_bytes(book.title, TlvTypes.BOOK_TITLE)
            for author in book.authors:
                raw_book += string_to_bytes(author, TlvTypes.BOOK_AUTHORS)
            raw_book += string_to_bytes(book.publisher, TlvTypes.BOOK_PUBLISHER)
            raw_book += string_to_bytes(book.publishedDate, TlvTypes.BOOK_PUBLISHED_DATE)
            for category in book.categories:
                raw_book += string_to_bytes(category, TlvTypes.BOOK_CATEGORIES)

            raw_chunk += code_to_bytes(TlvTypes.BOOK)
            raw_chunk += int.to_bytes(len(raw_book), SIZE_LENGTH, 'big')
            raw_chunk += raw_book

        #result = code_to_bytes(TlvTypes.BOOK_CHUNK)
        #result += int.to_bytes(len(chunk), SIZE_LENGTH, 'big')
        result = raw_chunk

        return result
