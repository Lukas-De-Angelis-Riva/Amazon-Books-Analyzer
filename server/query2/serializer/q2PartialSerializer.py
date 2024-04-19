from utils.serializer.serializer import Serializer
from utils.protocol import SIZE_LENGTH
from utils.protocol import string_to_bytes, string_from_bytes
from utils.protocol import intarr_to_bytes, intarr_from_bytes
from utils.protocol import code_to_bytes
from dto.q2Partial import Q2Partial

class Q2PartialTypes():
    CHUNK = 0
    PARTIAL = 1
    AUTHOR = 2
    DECADES = 3

class Q2PartialSerializer(Serializer):

    def make_raw_dict(self):
        return {
            Q2PartialTypes.AUTHOR : b'',
            Q2PartialTypes.DECADES : b'',
        }

    def from_raw_dict(self, raw_dict):
        # Verification: all books field should be received
        assert raw_dict[Q2PartialTypes.AUTHOR], "Invalid Q2Partial: no author provided"
        assert raw_dict[Q2PartialTypes.DECADES], "Invalid Q2Partial: no decades provided"

        return Q2Partial(
            author = string_from_bytes(raw_dict[Q2PartialTypes.AUTHOR]),
            decades = intarr_from_bytes(raw_dict[Q2PartialTypes.DECADES]),
        )

    def to_bytes(self, chunk: list):
        raw_chunk = b''

        for partial in chunk:
            raw_partial = b''
            raw_partial += string_to_bytes(partial.author, Q2PartialTypes.AUTHOR)
            raw_partial += intarr_to_bytes(partial.decades, Q2PartialTypes.DECADES)

            raw_chunk += code_to_bytes(Q2PartialTypes.PARTIAL)
            raw_chunk += int.to_bytes(len(raw_partial), SIZE_LENGTH, 'big') 
            raw_chunk += raw_partial

        result = code_to_bytes(Q2PartialTypes.CHUNK)
        result += int.to_bytes(len(chunk), SIZE_LENGTH, 'big') 
        result += raw_chunk

        return result