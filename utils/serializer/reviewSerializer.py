from utils.serializer.serializer import Serializer
from utils.protocol import TlvTypes, SIZE_LENGTH
from utils.protocol import string_to_bytes, string_from_bytes
from utils.protocol import integer_to_bytes, integer_from_bytes
from utils.protocol import float_to_bytes, float_from_bytes
from utils.protocol import code_to_bytes
from model.review import Review

class ReviewSerializer(Serializer):

    def make_raw_dict(self):
        return {
            TlvTypes.REVIEW_ID : b'',
            TlvTypes.REVIEW_TITLE : b'',
            TlvTypes.REVIEW_SCORE : b'',
            TlvTypes.REVIEW_TEXT : b'',
        }


    def from_raw_dict(self, raw_dict):
        # Verification: all reviews field should be received
        assert raw_dict[TlvTypes.REVIEW_ID], "Invalid review: no id provided"
        assert raw_dict[TlvTypes.REVIEW_TITLE], "Invalid review: no title provided"
        assert raw_dict[TlvTypes.REVIEW_SCORE], "Invalid review: no score provided"
        assert raw_dict[TlvTypes.REVIEW_TEXT], "Invalid review: no text provided"

        return Review(
            id = string_from_bytes(raw_dict[TlvTypes.REVIEW_ID]),
            title = string_from_bytes(raw_dict[TlvTypes.REVIEW_TITLE]),
            score = float_from_bytes(raw_dict[TlvTypes.REVIEW_SCORE]),
            text = string_from_bytes(raw_dict[TlvTypes.REVIEW_TEXT]),
        )

    def to_bytes(self, chunk: list):
        raw_chunk = b''

        for book in chunk:
            raw_book = b''
            raw_book += string_to_bytes(book.id, TlvTypes.REVIEW_ID)
            raw_book += string_to_bytes(book.title, TlvTypes.REVIEW_TITLE)
            raw_book += float_to_bytes(book.score, TlvTypes.REVIEW_SCORE)
            raw_book += string_to_bytes(book.text, TlvTypes.REVIEW_TEXT)
            
            raw_chunk += code_to_bytes(TlvTypes.REVIEW)
            raw_chunk += int.to_bytes(len(raw_book), SIZE_LENGTH, 'big') 
            raw_chunk += raw_book

        result = code_to_bytes(TlvTypes.REVIEW_CHUNK)
        result += int.to_bytes(len(chunk), SIZE_LENGTH, 'big') 
        result += raw_chunk

        return result