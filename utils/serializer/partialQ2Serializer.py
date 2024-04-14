from utils.serializer.serializer import Serializer
from utils.protocol import TlvTypes, SIZE_LENGTH
from utils.protocol import string_to_bytes, string_from_bytes
from utils.protocol import intarr_to_bytes, intarr_from_bytes
from utils.protocol import code_to_bytes
from utils.q2Partial import Q2Partial

class PartialQ2Serializer(Serializer):

    def make_raw_dict(self):
        return {
            TlvTypes.Q2_PARTIAL_AUTHOR : b'',
            TlvTypes.Q2_PARTIAL_DECADES : b'',
        }

    def from_raw_dict(self, raw_dict):
        # Verification: all books field should be received
        assert raw_dict[TlvTypes.Q2_PARTIAL_AUTHOR], "Invalid Q2Partial: no author provided"
        assert raw_dict[TlvTypes.Q2_PARTIAL_DECADES], "Invalid Q2Partial: no decades provided"

        return Q2Partial(
            author = string_from_bytes(raw_dict[TlvTypes.Q2_PARTIAL_AUTHOR]),
            decades = intarr_from_bytes(raw_dict[TlvTypes.Q2_PARTIAL_DECADES]),
        )

    def to_bytes(self, chunk: list):
        raw_chunk = b''

        for partial in chunk:
            raw_partial = b''
            raw_partial += string_to_bytes(partial.author, TlvTypes.Q2_PARTIAL_AUTHOR)
            raw_partial += intarr_to_bytes(partial.decades, TlvTypes.Q2_PARTIAL_DECADES)

            raw_chunk += code_to_bytes(TlvTypes.Q2_PARTIAL)
            raw_chunk += int.to_bytes(len(raw_partial), SIZE_LENGTH, 'big') 
            raw_chunk += raw_partial

        result = code_to_bytes(TlvTypes.Q2_PARTIAL_CHUNK)
        result += int.to_bytes(len(chunk), SIZE_LENGTH, 'big') 
        result += raw_chunk

        return result