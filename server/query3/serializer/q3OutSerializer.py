from utils.serializer.serializer import Serializer
from utils.protocol import SIZE_LENGTH
from utils.protocol import string_to_bytes, string_from_bytes
from utils.protocol import code_to_bytes
from dto.q3Result import Q3Result


class Q3OutTypes():
    CHUNK = 30
    RESULT = 31
    TITLE = 32
    AUTHORS = 33


class Q3OutSerializer(Serializer):

    def make_raw_dict(self):
        return {
            Q3OutTypes.TITLE: b'',
            Q3OutTypes.AUTHORS: [],
        }

    def from_raw_dict(self, raw_dict):
        assert raw_dict[Q3OutTypes.TITLE], "Invalid Q3Result: no title provided"
        assert raw_dict[Q3OutTypes.AUTHORS], "Invalid Q3Result: no authors provided"

        return Q3Result(
            title=string_from_bytes(raw_dict[Q3OutTypes.TITLE]),
            authors=[
                string_from_bytes(raw_author) for raw_author in raw_dict[Q3OutTypes.AUTHORS]
            ],
        )

    def to_bytes(self, chunk: list):
        raw_chunk = b''

        for result in chunk:
            raw_result = b''
            raw_result += string_to_bytes(result.title, Q3OutTypes.TITLE)
            for author in result.authors:
                raw_result += string_to_bytes(author, Q3OutTypes.AUTHORS)

            raw_chunk += code_to_bytes(Q3OutTypes.RESULT)
            raw_chunk += int.to_bytes(len(raw_result), SIZE_LENGTH, 'big')
            raw_chunk += raw_result

        result = code_to_bytes(Q3OutTypes.CHUNK)
        result += int.to_bytes(len(chunk), SIZE_LENGTH, 'big')
        result += raw_chunk

        return result
