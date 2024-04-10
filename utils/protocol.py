import struct

SIZE_LENGTH = 4

i = -1
def next():
    global i
    i+=1
    return i

class TlvTypes():
    # sizes
    SIZE_CODE_MSG = 4

    # types
    EOF = next()
    WAIT = next()
    POLL = next()
    
    BOOK_CHUNK = next()
    BOOK = next()
    BOOK_TITLE = next()
    BOOK_AUTHORS = next()
    BOOK_PUBLISHER = next()
    BOOK_PUBLISHED_DATE = next()
    BOOK_CATEGORIES = next()

    Q1_RESULT_CHUNK = next()
    Q1_RESULT = next()
    Q1_RESULT_TITLE = next()
    Q1_RESULT_AUTHORS = next()
    Q1_RESULT_PUBLISHER = next()

    REVIEW_CHUNK = next()
    REVIEW = next()
    ACK = next()
    
def is_eof(bytes):
    if len(bytes) == TlvTypes.SIZE_CODE_MSG:
        data = struct.unpack("!i",bytes)[0]
        return data == TlvTypes.EOF
    elif len(bytes) == TlvTypes.SIZE_CODE_MSG+SIZE_LENGTH:
        data, n = struct.unpack("!ii",bytes)
        return data == TlvTypes.EOF
    else:
        return False

def get_closed_peers(bytes):
    if len(bytes) == TlvTypes.SIZE_CODE_MSG + SIZE_LENGTH:
        data, n = struct.unpack("!ii", bytes)
        return n
    return -1

def make_wait():
    bytes = code_to_bytes(TlvTypes.WAIT)
    bytes += int.to_bytes(i, SIZE_LENGTH, 'big')
    return bytes

def make_eof(i = 0):
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

def float_to_bytes(f:float, code: int):
    bytes = code_to_bytes(code)
    bytes_f = struct.pack('!f', f)
    bytes += int.to_bytes(len(bytes_f), SIZE_LENGTH, 'big')
    bytes += bytes_f
    return bytes

def float_from_bytes(bytes_f):
    return struct.unpack('!f', bytes_f)[0]

class UnexpectedType(Exception):
    pass