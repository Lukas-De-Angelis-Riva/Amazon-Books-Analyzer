from utils.serializer.serializer import Serializer
from utils.protocol import SIZE_LENGTH
from utils.protocol import string_to_bytes, string_from_bytes
from utils.protocol import float_to_bytes, float_from_bytes
from utils.protocol import code_to_bytes
from model.review import Review


class Q3ReviewInTypes():
    CHUNK = 0
    REVIEW = 1
    TITLE = 2
    SCORE = 3


class Q3ReviewInSerializer(Serializer):
    def make_raw_dict(self):
        return {
            Q3ReviewInTypes.TITLE: b'',
            Q3ReviewInTypes.SCORE: b'',
        }

    def from_raw_dict(self, raw_dict):
        assert raw_dict[Q3ReviewInTypes.TITLE], "Invalid review: no title provided"
        assert raw_dict[Q3ReviewInTypes.SCORE], "Invalid review: no score provided"

        return Review(
            id="",
            title=string_from_bytes(raw_dict[Q3ReviewInTypes.TITLE]),
            score=float_from_bytes(raw_dict[Q3ReviewInTypes.SCORE]),
            text="",
        )

    def to_bytes(self, chunk: list):
        raw_chunk = b''

        for review in chunk:
            raw_review = b''
            raw_review += string_to_bytes(review.title, Q3ReviewInTypes.TITLE)
            raw_review += float_to_bytes(review.score, Q3ReviewInTypes.SCORE)

            raw_chunk += code_to_bytes(Q3ReviewInTypes.REVIEW)
            raw_chunk += int.to_bytes(len(raw_review), SIZE_LENGTH, 'big')
            raw_chunk += raw_review

        result = code_to_bytes(Q3ReviewInTypes.CHUNK)
        result += int.to_bytes(len(chunk), SIZE_LENGTH, 'big')
        result += raw_chunk

        return result
