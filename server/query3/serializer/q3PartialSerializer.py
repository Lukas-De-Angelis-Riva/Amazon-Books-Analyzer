from utils.serializer.serializer import Serializer
from utils.protocol import SIZE_LENGTH
from utils.protocol import integer_to_bytes, integer_from_bytes
from utils.protocol import string_to_bytes, string_from_bytes
from utils.protocol import float_to_bytes, float_from_bytes
from utils.protocol import code_to_bytes
from dto.q3Partial import Q3Partial


class Q3PartialTypes():
    CHUNK = 0
    PARTIAL = 1
    TITLE = 2
    AUTHORS = 3
    N = 4
    AVG = 5


class Q3PartialSerializer(Serializer):
    def make_raw_dict(self):
        return {
            Q3PartialTypes.TITLE: b'',
            Q3PartialTypes.AUTHORS: [],
            Q3PartialTypes.N: b'',
            Q3PartialTypes.AVG: b'',
        }

    def from_raw_dict(self, raw_dict):
        return Q3Partial(
            title=string_from_bytes(raw_dict[Q3PartialTypes.TITLE]),
            authors=[
                string_from_bytes(raw_author) for raw_author in raw_dict[Q3PartialTypes.AUTHORS]
            ],
            n=integer_from_bytes(raw_dict[Q3PartialTypes.N]),
            scoreAvg=float_from_bytes(raw_dict[Q3PartialTypes.AVG]),
        )

    def to_bytes(self, chunk: list):
        raw_chunk = b''

        for partialQ3 in chunk:
            raw_partial = b''
            raw_partial += string_to_bytes(partialQ3.title, Q3PartialTypes.TITLE)
            for author in partialQ3.authors:
                raw_partial += string_to_bytes(author, Q3PartialTypes.AUTHORS)
            raw_partial += integer_to_bytes(partialQ3.n, Q3PartialTypes.N)
            raw_partial += float_to_bytes(partialQ3.scoreAvg, Q3PartialTypes.AVG)

            raw_chunk += code_to_bytes(Q3PartialTypes.PARTIAL)
            raw_chunk += int.to_bytes(len(raw_partial), SIZE_LENGTH, 'big')
            raw_chunk += raw_partial

        result = code_to_bytes(Q3PartialTypes.CHUNK)
        result += int.to_bytes(len(chunk), SIZE_LENGTH, 'big')
        result += raw_chunk

        return result
