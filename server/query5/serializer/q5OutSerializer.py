from utils.serializer.serializer import Serializer
from utils.protocol import SIZE_LENGTH
from utils.protocol import string_to_bytes, string_from_bytes
from utils.protocol import code_to_bytes


class Q5OutTypes():
    CHUNK = 0
    RESULT = 1
    TITLE = 2


class Q5OutSerializer(Serializer):
    def make_raw_dict(self):
        return {
            Q5OutTypes.TITLE: b'',
        }

    def from_raw_dict(self, raw_dict):
        assert raw_dict[Q5OutTypes.TITLE], "Invalid Q5Result: no title provided"

        return string_from_bytes(raw_dict[Q5OutTypes.TITLE])

    def to_bytes(self, chunk: list):
        raw_chunk = b''

        for title in chunk:
            raw_title = b''
            raw_title += string_to_bytes(title, Q5OutTypes.TITLE)

            raw_chunk += code_to_bytes(Q5OutTypes.RESULT)
            raw_chunk += int.to_bytes(len(raw_title), SIZE_LENGTH, 'big')
            raw_chunk += raw_title

        result = code_to_bytes(Q5OutTypes.CHUNK)
        result += int.to_bytes(len(chunk), SIZE_LENGTH, 'big')
        result += raw_chunk

        return result
