from utils.serializer.serializer import Serializer
from utils.protocol import SIZE_LENGTH
from utils.protocol import string_to_bytes, string_from_bytes
from utils.protocol import code_to_bytes
from model.book import Book

class Q5BookInTypes():
    CHUNK = 0
    BOOK = 1
    TITLE = 2
    CATEGORIES = 3

class Q5BookInSerializer(Serializer):
    def make_raw_dict(self):
        return {
            Q5BookInTypes.TITLE : b'',
            Q5BookInTypes.CATEGORIES : [],
        }


    def from_raw_dict(self, raw_dict):
        assert raw_dict[Q5BookInTypes.TITLE], "Invalid book: no title provided"
        assert raw_dict[Q5BookInTypes.CATEGORIES], "Invalid book: no categories provided"

        return Book(
            title = string_from_bytes(raw_dict[Q5BookInTypes.TITLE]),
            authors = [],
            publisher = "",
            publishedDate = "",
            categories = [
                string_from_bytes(raw_category) for raw_category in raw_dict[Q5BookInTypes.CATEGORIES]
            ],
        )

    def to_bytes(self, chunk: list):
        raw_chunk = b''

        for book in chunk:
            raw_book = b''
            raw_book += string_to_bytes(book.title, Q5BookInTypes.TITLE)
            for category in book.categories:
                raw_book += string_to_bytes(category, Q5BookInTypes.CATEGORIES)

            raw_chunk += code_to_bytes(Q5BookInTypes.BOOK)
            raw_chunk += int.to_bytes(len(raw_book), SIZE_LENGTH, 'big')
            raw_chunk += raw_book

        result = code_to_bytes(Q5BookInTypes.CHUNK)
        result += int.to_bytes(len(chunk), SIZE_LENGTH, 'big')
        result += raw_chunk

        return result