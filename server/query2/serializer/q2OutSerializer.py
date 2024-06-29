from utils.serializer.serializer import Serializer
from utils.protocol import SIZE_LENGTH
from utils.protocol import string_to_bytes, string_from_bytes
from utils.protocol import code_to_bytes


class Q2OutTypes():
    CHUNK = 20
    RESULT = 21
    AUTHOR = 22


class Q2OutSerializer(Serializer):

    def make_raw_dict(self):
        return {
            Q2OutTypes.AUTHOR: b'',
        }

    def from_raw_dict(self, raw_dict):
        assert raw_dict[Q2OutTypes.AUTHOR], "Invalid Q2Result: no author provided"

        return string_from_bytes(raw_dict[Q2OutTypes.AUTHOR])

    def to_bytes(self, chunk: list):
        raw_chunk = b''

        for author in chunk:
            raw_author = b''
            raw_author += string_to_bytes(author, Q2OutTypes.AUTHOR)

            raw_chunk += code_to_bytes(Q2OutTypes.RESULT)
            raw_chunk += int.to_bytes(len(raw_author), SIZE_LENGTH, 'big')
            raw_chunk += raw_author

        result = code_to_bytes(Q2OutTypes.CHUNK)
        result += int.to_bytes(len(chunk), SIZE_LENGTH, 'big')
        result += raw_chunk

        return result
