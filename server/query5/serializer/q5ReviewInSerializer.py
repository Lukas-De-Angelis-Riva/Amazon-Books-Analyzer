from utils.serializer.serializer import Serializer
from utils.protocol import SIZE_LENGTH
from utils.protocol import string_to_bytes, string_from_bytes
from utils.protocol import code_to_bytes
from model.review import Review

class Q5ReviewInTypes():
    CHUNK = 0
    REVIEW = 1
    TITLE = 2
    TEXT = 3

class Q5ReviewInSerializer(Serializer):
    def make_raw_dict(self):
        return {
            Q5ReviewInTypes.TITLE : b'',
            Q5ReviewInTypes.TEXT: b'',
        }


    def from_raw_dict(self, raw_dict):
        assert raw_dict[Q5ReviewInTypes.TITLE], "Invalid review: no title provided"
        assert raw_dict[Q5ReviewInTypes.TEXT], "Invalid review: no text provided"

        return Review(
            id = 0,
            title = string_from_bytes(raw_dict[Q5ReviewInTypes.TITLE]),
            score = 0.0,
            text = string_from_bytes(raw_dict[Q5ReviewInTypes.TEXT]),
        )

    def to_bytes(self, chunk: list):
        raw_chunk = b''

        for review in chunk:
            raw_review = b''
            raw_review += string_to_bytes(review.title, Q5ReviewInTypes.TITLE)
            raw_review += string_to_bytes(review.text, Q5ReviewInTypes.TEXT)

            raw_chunk += code_to_bytes(Q5ReviewInTypes.REVIEW)
            raw_chunk += int.to_bytes(len(raw_review), SIZE_LENGTH, 'big') 
            raw_chunk += raw_review

        result = code_to_bytes(Q5ReviewInTypes.CHUNK)
        result += int.to_bytes(len(chunk), SIZE_LENGTH, 'big') 
        result += raw_chunk

        return result