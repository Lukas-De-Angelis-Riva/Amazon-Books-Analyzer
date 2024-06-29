from utils.serializer.serializer import Serializer
from utils.protocol import SIZE_LENGTH
from utils.protocol import string_to_bytes, string_from_bytes
from utils.protocol import code_to_bytes
from model.book import Book


class Q1OutTypes():
    CHUNK = 10
    RESULT = 11
    TITLE = 12
    AUTHORS = 13
    PUBLISHER = 14


class Q1OutSerializer(Serializer):

    def make_raw_dict(self):
        return {
            Q1OutTypes.TITLE: b'',
            Q1OutTypes.AUTHORS: [],
            Q1OutTypes.PUBLISHER: b'',
        }

    def from_raw_dict(self, raw_dict):
        # Verification: all books field should be received
        assert raw_dict[Q1OutTypes.TITLE], "Invalid Q1Result: no title provided"
        assert raw_dict[Q1OutTypes.AUTHORS], "Invalid Q1Result: no authors provided"
        assert raw_dict[Q1OutTypes.PUBLISHER], "Invalid Q1Result: no publisher provided"

        return Book(
            title=string_from_bytes(raw_dict[Q1OutTypes.TITLE]),
            authors=[
                string_from_bytes(raw_author) for raw_author in raw_dict[Q1OutTypes.AUTHORS]
            ],
            publisher=string_from_bytes(raw_dict[Q1OutTypes.PUBLISHER]),
        )

    def to_bytes(self, chunk: list):
        raw_chunk = b''

        for book in chunk:
            raw_book = b''
            raw_book += string_to_bytes(book.title, Q1OutTypes.TITLE)
            for author in book.authors:
                raw_book += string_to_bytes(author, Q1OutTypes.AUTHORS)
            raw_book += string_to_bytes(book.publisher, Q1OutTypes.PUBLISHER)

            raw_chunk += code_to_bytes(Q1OutTypes.RESULT)
            raw_chunk += int.to_bytes(len(raw_book), SIZE_LENGTH, 'big')
            raw_chunk += raw_book

        result = code_to_bytes(Q1OutTypes.CHUNK)
        result += int.to_bytes(len(chunk), SIZE_LENGTH, 'big')
        result += raw_chunk

        return result
