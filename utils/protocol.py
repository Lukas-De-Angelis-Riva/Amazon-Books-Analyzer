import struct
import uuid

SIZE_LENGTH = 4

i = -1


def next():
    global i
    i += 1
    return i


class TlvTypes():
    # sizes
    SIZE_CODE_MSG = 4
    SIZE_UUID_MSG = 16

    # types
    EOF = next()
    WAIT = next()
    POLL = next()
    ACK = next()
    UUID = next()

    BOOK_CHUNK = next()
    BOOK = next()
    BOOK_TITLE = next()
    BOOK_AUTHORS = next()
    BOOK_PUBLISHER = next()
    BOOK_PUBLISHED_DATE = next()
    BOOK_CATEGORIES = next()

    REVIEW_CHUNK = next()
    REVIEW = next()
    REVIEW_ID = next()
    REVIEW_TITLE = next()
    REVIEW_SCORE = next()
    REVIEW_TEXT = next()

    LINE_CHUNK = next()
    LINE = next()
    LINE_RAW = next()


def is_eof(bytes):
    if len(bytes) >= TlvTypes.SIZE_CODE_MSG:
        t = struct.unpack("!i", bytes[0:TlvTypes.SIZE_CODE_MSG])[0]
        return t == TlvTypes.EOF
    return False


def get_eof_argument(bytes):
    if len(bytes) == TlvTypes.SIZE_CODE_MSG + SIZE_LENGTH:
        data, n = struct.unpack("!ii", bytes)
        return n
    return -1


def get_eof_argument(bytes):
    if len(bytes) == TlvTypes.SIZE_CODE_MSG + SIZE_LENGTH:
        data, n = struct.unpack("!ii", bytes)
        return n
    return -1


def get_closed_peers(bytes):
    if len(bytes) == TlvTypes.SIZE_CODE_MSG + SIZE_LENGTH:
        data, n = struct.unpack("!ii", bytes)
        return n
    return -1


def make_wait():
    bytes = code_to_bytes(TlvTypes.WAIT)
    bytes += int.to_bytes(i, SIZE_LENGTH, 'big')
    return bytes


def get_eof_argument2(bytes):
    if len(bytes) == TlvTypes.SIZE_CODE_MSG + 3 * SIZE_LENGTH:
        t, total, worked, sent = struct.unpack("!iiii", bytes)
        return total, worked, sent
    return None, None, None


# total: total amount of individual data received
# worked: total amount of data worked by your peers
# sent: total amount of data sent by your peers to next stage
def make_eof2(total, worked, sent):
    bytes = code_to_bytes(tlvtypes.eof)
    bytes += int.to_bytes(total, size_length, 'big')
    bytes += int.to_bytes(worked, size_length, 'big')
    bytes += int.to_bytes(sent, size_length, 'big')
    return bytes


def make_token(peer_id, total, worked, sent):
    bytes = code_to_bytes(tlvtypes.eof)
    bytes += int.to_bytes(total, size_length, 'big')
    bytes += int.to_bytes(worked, size_length, 'big')
    bytes += int.to_bytes(sent, size_length, 'big')
    return bytes



def make_eof(i=0):
    bytes = code_to_bytes(TlvTypes.EOF)
    bytes += int.to_bytes(i, SIZE_LENGTH, 'big')
    return bytes


def make_heartbeat():
    bytes = code_to_bytes(TlvTypes.HEARTBEAT)
    bytes += int.to_bytes(0, SIZE_LENGTH, 'big')
    return bytes


def code_to_bytes(code: int):
    return int.to_bytes(code, TlvTypes.SIZE_CODE_MSG, 'big')


def integer_to_bytes(i: int, code: int):
    bytes = code_to_bytes(code)
    bytes += int.to_bytes(SIZE_LENGTH, SIZE_LENGTH, 'big')
    bytes += int.to_bytes(i, SIZE_LENGTH, 'big')
    return bytes


def integer_from_bytes(bytes_i):
    return int.from_bytes(bytes_i, 'big')


def string_to_bytes(s: str, code: int):
    bytes = code_to_bytes(code)
    bytes_s = s.encode('utf-8')
    bytes += int.to_bytes(len(bytes_s), SIZE_LENGTH, 'big')
    bytes += bytes_s
    return bytes


def string_from_bytes(bytes_s):
    return bytes_s.decode('utf-8')


def float_to_bytes(f: float, code: int):
    bytes = code_to_bytes(code)
    bytes_f = struct.pack('!f', f)
    bytes += int.to_bytes(len(bytes_f), SIZE_LENGTH, 'big')
    bytes += bytes_f
    return bytes


def float_from_bytes(bytes_f):
    return struct.unpack('!f', bytes_f)[0]


def intarr_to_bytes(int_array, code: int):
    bytes = code_to_bytes(code)
    bytes_arr = b''
    for i in int_array:
        bytes_arr += int.to_bytes(i, SIZE_LENGTH, 'big')
    bytes += int.to_bytes(len(bytes_arr), SIZE_LENGTH, 'big')
    bytes += bytes_arr
    return bytes

def intarr_from_bytes(bytes_arr):
    array = []
    n = len(bytes_arr)//SIZE_LENGTH
    for i in range(0, n):
        bytes_i = bytes_arr[i*SIZE_LENGTH:(i+1)*SIZE_LENGTH]
        array.append(int.from_bytes(bytes_i, 'big'))
    return array


def make_msg_id():
    return uuid.uuid4().bytes

def msg_id_from_bytes(raw):
    return uuid.UUID(bytes=raw)

class UnexpectedType(Exception):
    pass
