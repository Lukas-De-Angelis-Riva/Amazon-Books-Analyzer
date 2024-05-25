from utils.serializer.serializer import Serializer
from utils.protocol import SIZE_LENGTH
from utils.protocol import integer_to_bytes, integer_from_bytes
from utils.protocol import string_to_bytes, string_from_bytes
from utils.protocol import float_to_bytes, float_from_bytes
from utils.protocol import code_to_bytes
from dto.q5Partial import Q5Partial


class Q5PartialTypes():
    CHUNK = 0
    PARTIAL = 1
    TITLE = 2
    N = 3
    AVG = 4


class Q5PartialSerializer(Serializer):
    def make_raw_dict(self):
        return {
            Q5PartialTypes.TITLE: b'',
            Q5PartialTypes.N: b'',
            Q5PartialTypes.AVG: b'',
        }

    def from_raw_dict(self, raw_dict):
        return Q5Partial(
            title=string_from_bytes(raw_dict[Q5PartialTypes.TITLE]),
            n=integer_from_bytes(raw_dict[Q5PartialTypes.N]),
            sentimentAvg=float_from_bytes(raw_dict[Q5PartialTypes.AVG]),
        )

    def to_bytes(self, chunk: list):
        raw_chunk = b''

        for partialQ5 in chunk:
            raw_partial = b''
            raw_partial += string_to_bytes(partialQ5.title, Q5PartialTypes.TITLE)
            raw_partial += integer_to_bytes(partialQ5.n, Q5PartialTypes.N)
            raw_partial += float_to_bytes(partialQ5.sentimentAvg, Q5PartialTypes.AVG)

            raw_chunk += code_to_bytes(Q5PartialTypes.PARTIAL)
            raw_chunk += int.to_bytes(len(raw_partial), SIZE_LENGTH, 'big')
            raw_chunk += raw_partial

        result = code_to_bytes(Q5PartialTypes.CHUNK)
        result += int.to_bytes(len(chunk), SIZE_LENGTH, 'big')
        result += raw_chunk

        return result
