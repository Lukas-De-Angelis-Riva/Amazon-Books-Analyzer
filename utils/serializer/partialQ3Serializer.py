from utils.serializer.serializer import Serializer
from utils.protocol import TlvTypes, SIZE_LENGTH
from utils.protocol import integer_to_bytes, integer_from_bytes
from utils.protocol import string_to_bytes, string_from_bytes
from utils.protocol import float_to_bytes, float_from_bytes
from utils.protocol import code_to_bytes
from utils.q3Partial import Q3Partial

class PartialQ3Serializer(Serializer):

    def make_raw_dict(self):
        return {
            TlvTypes.Q3_PARTIAL_TITLE: b'',
            TlvTypes.Q3_PARTIAL_AUTHORS: [],
            TlvTypes.Q3_PARTIAL_N: b'',
            TlvTypes.Q3_PARTIAL_AVG: b'',
        }

    def from_raw_dict(self, raw_dict):
        return Q3Partial(
            title=string_from_bytes(raw_dict[TlvTypes.Q3_PARTIAL_TITLE]),
            authors=[
                string_from_bytes(raw_author) for raw_author in raw_dict[TlvTypes.Q3_PARTIAL_AUTHORS]
            ],
            n=integer_from_bytes(raw_dict[TlvTypes.Q3_PARTIAL_N]),
            scoreAvg=float_from_bytes(raw_dict[TlvTypes.Q3_PARTIAL_AVG]),
        )

    def to_bytes(self, chunk: list):
        raw_chunk = b''

        for partialQ3 in chunk:
            raw_partial = b''
            raw_partial += string_to_bytes(partialQ3.title, TlvTypes.Q3_PARTIAL_TITLE)
            for author in partialQ3.authors:
                raw_partial += string_to_bytes(author, TlvTypes.Q3_PARTIAL_AUTHORS)
            raw_partial += integer_to_bytes(partialQ3.n, TlvTypes.Q3_PARTIAL_N)
            raw_partial += float_to_bytes(partialQ3.scoreAvg, TlvTypes.Q3_PARTIAL_AVG)

            raw_chunk += code_to_bytes(TlvTypes.Q3_PARTIAL)
            raw_chunk += int.to_bytes(len(raw_partial), SIZE_LENGTH, 'big')
            raw_chunk += raw_partial

        result = code_to_bytes(TlvTypes.Q3_PARTIAL_CHUNK)
        result += int.to_bytes(len(chunk), SIZE_LENGTH, 'big')
        result += raw_chunk

        return result