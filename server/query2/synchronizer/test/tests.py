import io
import unittest

from utils.serializer.q2OutSerializer import Q2OutSerializer    # type: ignore


class TestUtils(unittest.TestCase):
    def test_resultq2serializer(self):
        serializer = Q2OutSerializer()

        result1 = 'Dennis Ritchie'
        result2 = 'Brian Kernighan'
        result3 = 'Andrew S. Tanenbaum'

        chunk = serializer.to_bytes([result1, result2, result3])
        reader = io.BytesIO(chunk)
        serial = serializer.from_chunk(reader)

        _result1 = serial[0]
        _result2 = serial[1]
        _result3 = serial[2]

        assert result1 == _result1
        assert result2 == _result2
        assert result3 == _result3


if __name__ == '__main__':
    unittest.main()
