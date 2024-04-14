import logging

from utils.serializer.serializer import Serializer
from utils.protocol import TlvTypes, SIZE_LENGTH
from utils.protocol import string_to_bytes, string_from_bytes
from utils.protocol import code_to_bytes

class ResultQ2Serializer(Serializer):

    def make_raw_dict(self):
        return {
            TlvTypes.Q2_RESULT_AUTHOR : b'',
        }

    def from_raw_dict(self, raw_dict):
        # Verification: all books field should be received
        assert raw_dict[TlvTypes.Q2_RESULT_AUTHOR], "Invalid Q2Result: no author provided"

        return string_from_bytes(raw_dict[TlvTypes.Q2_RESULT_AUTHOR])

    def to_bytes(self, chunk: list):
        raw_chunk = b''

        for author in chunk:
            raw_author = b''
            raw_author += string_to_bytes(author, TlvTypes.Q2_RESULT_AUTHOR)

            raw_chunk += code_to_bytes(TlvTypes.Q2_RESULT)
            raw_chunk += int.to_bytes(len(raw_author), SIZE_LENGTH, 'big') 
            raw_chunk += raw_author

        result = code_to_bytes(TlvTypes.Q2_RESULT_CHUNK)
        result += int.to_bytes(len(chunk), SIZE_LENGTH, 'big') 
        result += raw_chunk

        return result