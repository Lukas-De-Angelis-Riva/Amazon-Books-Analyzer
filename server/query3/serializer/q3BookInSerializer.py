from utils.serializer.serializer import Serializer
from utils.protocol import SIZE_LENGTH
from utils.protocol import string_to_bytes, string_from_bytes
from utils.protocol import code_to_bytes
from model.book import Book

class Q3BookInTypes():
    CHUNK = 0
    BOOK = 1
    TITLE = 2
    AUTHORS = 3
    PUBLISHED_DATE = 4

class Q3BookInSerializer(Serializer):
    def make_raw_dict(self):
        return {
            Q3BookInTypes.TITLE : b'',
            Q3BookInTypes.AUTHORS : [],
            Q3BookInTypes.PUBLISHED_DATE: b'',
        }


    def from_raw_dict(self, raw_dict):
        assert raw_dict[Q3BookInTypes.TITLE], "Invalid book: no title provided"
        assert raw_dict[Q3BookInTypes.AUTHORS], "Invalid book: no authors provided"
        assert raw_dict[Q3BookInTypes.PUBLISHED_DATE], "Invalid book: no date provided"

        return Book(
            title = string_from_bytes(raw_dict[Q3BookInTypes.TITLE]),
            authors = [
                string_from_bytes(raw_author) for raw_author in raw_dict[Q3BookInTypes.AUTHORS]
            ],
            publisher = "",
            publishedDate = string_from_bytes(raw_dict[Q3BookInTypes.PUBLISHED_DATE]),
            categories = [],
        )

    def to_bytes(self, chunk: list):
        raw_chunk = b''

        for book in chunk:
            raw_book = b''
            raw_book += string_to_bytes(book.title, Q3BookInTypes.TITLE)
            for author in book.authors:
                raw_book += string_to_bytes(author, Q3BookInTypes.AUTHORS)
            raw_book += string_to_bytes(book.publishedDate, Q3BookInTypes.PUBLISHED_DATE)
            
            raw_chunk += code_to_bytes(Q3BookInTypes.BOOK)
            raw_chunk += int.to_bytes(len(raw_book), SIZE_LENGTH, 'big')
            raw_chunk += raw_book

        result = code_to_bytes(Q3BookInTypes.CHUNK)
        result += int.to_bytes(len(chunk), SIZE_LENGTH, 'big')
        result += raw_chunk

        return result