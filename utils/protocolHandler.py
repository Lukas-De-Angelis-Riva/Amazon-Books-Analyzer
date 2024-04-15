import struct

from utils.TCPhandler import TCPHandler
from utils.protocol import TlvTypes, UnexpectedType, SIZE_LENGTH, is_eof, make_eof, make_heartbeat
from utils.serializer.bookSerializer import BookSerializer
from utils.serializer.reviewSerializer import ReviewSerializer
from utils.serializer.lineSerializer import LineSerializer

class ProtocolHandler:
    def __init__(self, socket):
        self.TCPHandler = TCPHandler(socket)
        self.book_serializer = BookSerializer()
        self.review_serializer = ReviewSerializer()
        self.line_serializer = LineSerializer()
        self.eof_books_received = False

    def wait_confimation(self):
        type_encode_raw = self.TCPHandler.read(TlvTypes.SIZE_CODE_MSG)
        type_encode = struct.unpack('!i', type_encode_raw)[0]
        assert type_encode == TlvTypes.ACK, f'Unexpected type: expected: ACK({TlvTypes.ACK}), received {type_encode}'

    def ack(self):
        bytes = int.to_bytes(TlvTypes.ACK, TlvTypes.SIZE_CODE_MSG, 'big')
        result = self.TCPHandler.send_all(bytes)
        assert result == len(bytes), f'TCP Error: cannot send ACK'

    def send_eof(self):
        eof = make_eof(0)
        result = self.TCPHandler.send_all(eof)
        assert result == len(eof), f'TCP Error: cannot send EOF'

    def send_book_eof(self):
        self.send_eof()
        self.wait_confimation()

    def send_review_eof(self):
        self.send_eof()
        self.wait_confimation()

    def send_books(self, books):
        bytes = self.book_serializer.to_bytes(books)
        result = self.TCPHandler.send_all(bytes)
        assert result == len(bytes), f'Cannot send all bytes {result} != {len(bytes)}'
        self.wait_confimation()

    def send_reviews(self, reviews):
        bytes = self.review_serializer.to_bytes(reviews)
        result = self.TCPHandler.send_all(bytes)
        assert result == len(bytes), f'Cannot send all bytes {result} != {len(bytes)}'
        self.wait_confimation()

    def read_tl(self):
        """
        Reads the Type and Length of TLV from self.TCPHandler and returns both.
        It reads a fixed amount of bytes (SIZE_CODE_MSG+SIZE_LENGTH)
        """
        _type_raw = self.TCPHandler.read(TlvTypes.SIZE_CODE_MSG)
        _type = struct.unpack('!i',_type_raw)[0]

        _len_raw = self.TCPHandler.read(SIZE_LENGTH)
        _len = struct.unpack('!i', _len_raw)[0]

        return _type, _len

    def read(self):
        tlv_type, tlv_len = self.read_tl()

        if tlv_type in [TlvTypes.EOF, TlvTypes.ACK, TlvTypes.WAIT, TlvTypes.POLL]:
            return tlv_type, None

        elif tlv_type == TlvTypes.BOOK_CHUNK:
            return TlvTypes.BOOK_CHUNK, self.book_serializer.from_chunk(self.TCPHandler, header=False, n_chunks=tlv_len)

        elif tlv_type == TlvTypes.REVIEW_CHUNK:
            return TlvTypes.REVIEW_CHUNK, self.review_serializer.from_chunk(self.TCPHandler, header=False, n_chunks=tlv_len)

        elif tlv_type == TlvTypes.LINE_CHUNK:
            return TlvTypes.LINE_CHUNK, self.line_serializer.from_chunk(self.TCPHandler, header=False, n_chunks=tlv_len)
        
        else:
            raise UnexpectedType()

    def poll_results(self):
        bytes = int.to_bytes(TlvTypes.POLL, TlvTypes.SIZE_CODE_MSG, 'big')
        result = self.TCPHandler.send_all(bytes)
        assert result == len(bytes), f'TCP Error: cannot send POLL'
        return self.read()
    
    def is_result_eof(self, t):
        return t == TlvTypes.EOF

    def is_ack(self, t):
        return t == TlvTypes.ACK
    
    def is_result_wait(self, t):
        return t == TlvTypes.WAIT

    def is_results(self, tlv_type):
       return tlv_type == TlvTypes.LINE_CHUNK

    def is_book_eof(self, t):
        if self.eof_books_received:
            return False
        elif t == TlvTypes.EOF:
            self.eof_books_received = True
            return True
        return False

    def is_review_eof(self, t):
        if not self.eof_books_received:
            return False
        elif t == TlvTypes.EOF:
            return True
        return False

    def is_book(self, tlv_type):
       return tlv_type == TlvTypes.BOOK_CHUNK

    def is_review(self, tlv_type):
        return tlv_type == TlvTypes.REVIEW_CHUNK
    
    def close(self):
        return
        # cerrar la conexion