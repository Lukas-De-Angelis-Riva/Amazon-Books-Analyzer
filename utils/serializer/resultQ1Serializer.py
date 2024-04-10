from utils.serializer.serializer import Serializer
from utils.protocol import TlvTypes, SIZE_LENGTH
from utils.protocol import string_to_bytes, string_from_bytes
from utils.protocol import code_to_bytes
from model.book import Book

class ResultQ1Serializer(Serializer):

    def make_raw_dict(self):
        return {
            TlvTypes.Q1_RESULT_TITLE : b'',
            TlvTypes.Q1_RESULT_AUTHORS : [],
            TlvTypes.Q1_RESULT_PUBLISHER : b'',
        }

    def from_raw_dict(self, raw_dict):
        # Verification: all books field should be received
        assert raw_dict[TlvTypes.Q1_RESULT_TITLE], "Invalid Q1Result: no title provided"
        assert raw_dict[TlvTypes.Q1_RESULT_AUTHORS], "Invalid Q1Result: no authors provided"
        assert raw_dict[TlvTypes.Q1_RESULT_PUBLISHER], "Invalid Q1Result: no publisher provided"

        return Book(
            title = string_from_bytes(raw_dict[TlvTypes.Q1_RESULT_TITLE]),
            authors = [
                string_from_bytes(raw_author) for raw_author in raw_dict[TlvTypes.Q1_RESULT_AUTHORS]
            ],
            publisher = string_from_bytes(raw_dict[TlvTypes.Q1_RESULT_PUBLISHER]),
        )

    def to_bytes(self, chunk: list):
        raw_chunk = b''

        for book in chunk:
            raw_book = b''
            raw_book += string_to_bytes(book.title, TlvTypes.Q1_RESULT_TITLE)
            for author in book.authors:
                raw_book += string_to_bytes(author, TlvTypes.Q1_RESULT_AUTHORS)
            raw_book += string_to_bytes(book.publisher, TlvTypes.Q1_RESULT_PUBLISHER)
            
            raw_chunk += code_to_bytes(TlvTypes.Q1_RESULT)
            raw_chunk += int.to_bytes(len(raw_book), SIZE_LENGTH, 'big') 
            raw_chunk += raw_book

        result = code_to_bytes(TlvTypes.Q1_RESULT_CHUNK)
        result += int.to_bytes(len(chunk), SIZE_LENGTH, 'big') 
        result += raw_chunk

        return result