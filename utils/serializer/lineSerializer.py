from utils.serializer.serializer import Serializer
from utils.protocol import TlvTypes, SIZE_LENGTH
from utils.protocol import string_to_bytes, string_from_bytes
from utils.protocol import code_to_bytes

class LineSerializer(Serializer):
    def make_raw_dict(self):
        return {
            TlvTypes.LINE_RAW: b'',
        }

    def from_raw_dict(self, raw_dict):
        return string_from_bytes(raw_dict[TlvTypes.LINE_RAW])

    def to_bytes(self, chunk: list):
        raw_chunk = b''

        for line in chunk:
            raw_result = b''
            raw_result += string_to_bytes(line, TlvTypes.LINE_RAW)

            raw_chunk += code_to_bytes(TlvTypes.LINE)
            raw_chunk += int.to_bytes(len(raw_result), SIZE_LENGTH, 'big')
            raw_chunk += raw_result

        result = code_to_bytes(TlvTypes.LINE_CHUNK)
        result += int.to_bytes(len(chunk), SIZE_LENGTH, 'big')
        result += raw_chunk

        return result