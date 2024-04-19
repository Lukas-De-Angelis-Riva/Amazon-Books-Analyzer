import logging

from utils.serializer.serializer import Serializer
from utils.protocol import TlvTypes, SIZE_LENGTH
from utils.protocol import string_to_bytes, string_from_bytes
from utils.protocol import code_to_bytes
from utils.q3Result import Q3Result

class ResultQ3Serializer(Serializer):

    def make_raw_dict(self):
        return {
            TlvTypes.Q3_RESULT_TITLE : b'',
            TlvTypes.Q3_RESULT_AUTHORS: [],
        }

    def from_raw_dict(self, raw_dict):
        # Verification: all books field should be received
        assert raw_dict[TlvTypes.Q3_RESULT_TITLE], "Invalid Q3Result: no title provided"
        assert raw_dict[TlvTypes.Q3_RESULT_AUTHORS], "Invalid Q3Result: no authors provided"

        return Q3Result(
            title=string_from_bytes(raw_dict[TlvTypes.Q3_RESULT_TITLE]),
            authors=[
                string_from_bytes(raw_author) for raw_author in raw_dict[TlvTypes.Q3_RESULT_AUTHORS]
            ],
        )

    def to_bytes(self, chunk: list):
        raw_chunk = b''

        for result in chunk:
            raw_result = b''
            raw_result += string_to_bytes(result.title, TlvTypes.Q3_RESULT_TITLE)
            for author in result.authors:
                raw_result += string_to_bytes(author, TlvTypes.Q3_RESULT_AUTHORS)

            raw_chunk += code_to_bytes(TlvTypes.Q3_RESULT)
            raw_chunk += int.to_bytes(len(raw_result), SIZE_LENGTH, 'big') 
            raw_chunk += raw_result

        result = code_to_bytes(TlvTypes.Q3_RESULT_CHUNK)
        result += int.to_bytes(len(chunk), SIZE_LENGTH, 'big') 
        result += raw_chunk

        return result