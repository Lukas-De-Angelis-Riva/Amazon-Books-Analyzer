import uuid
import struct

from utils.TCPhandler import TCPHandler
from utils.protocol import TlvTypes, UnexpectedType, SIZE_LENGTH, make_eof, code_to_bytes
from utils.protocol import UUID_LEN, MSG_ID_LEN, msg_id_from_bytes, make_msg_id
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
        err_msg = f'Unexpected type: expected: ACK({TlvTypes.ACK}), received {type_encode}' 
        assert type_encode == TlvTypes.ACK, err_msg

    def ack(self):
        bytes = int.to_bytes(TlvTypes.ACK, TlvTypes.SIZE_CODE_MSG, 'big')
        result = self.TCPHandler.send_all(bytes)
        assert result == len(bytes), 'TCP Error: cannot send ACK'

    def handshake(self, client_uuid):
        bytes = self.make_header(TlvTypes.UUID, UUID_LEN)
        bytes += client_uuid.bytes
        result = self.TCPHandler.send_all(bytes)
        assert result == len(bytes), 'TCP Error: cannot send UUID'
        self.wait_confimation()

    def wait_handshake(self):
        type, msg_id, payload_len = self.read_header()
        raw_uuid = self.TCPHandler.read(payload_len)
        assert type == TlvTypes.UUID, f'Unexpected type: expected: UUID({TlvTypes.UUID}), received {type}'
        client_uuid = uuid.UUID(bytes=raw_uuid)
        self.ack()
        return client_uuid

    def send_wait(self):
        wait = self.make_header(TlvTypes.WAIT, 0)
        result = self.TCPHandler.send_all(wait)
        assert result == len(wait), 'TCP Error: cannot send WAIT'
        self.wait_confimation()

    def poll_results(self):
        poll = self.make_header(TlvTypes.POLL, 0)
        result = self.TCPHandler.send_all(poll)
        assert result == len(poll), 'TCP Error: cannot send POLL'
        r = self.read()
        self.ack()
        return r

    def send_eof(self):
        #eof = make_eof(0)
        eof = self.make_header(TlvTypes.EOF, 0)
        result = self.TCPHandler.send_all(eof)
        assert result == len(eof), 'TCP Error: cannot send EOF'

    def send_book_eof(self):
        self.send_eof()
        self.wait_confimation()

    def send_review_eof(self):
        self.send_eof()
        self.wait_confimation()

    # TODO: maybe only one send_eof?
    def send_line_eof(self):
        self.send_eof()
        self.wait_confimation()

    def send_books(self, books):
        bytes = self.make_header(TlvTypes.BOOK_CHUNK, len(books))
        bytes += self.book_serializer.to_bytes(books)
        result = self.TCPHandler.send_all(bytes)
        assert result == len(bytes), f'Cannot send all bytes {result} != {len(bytes)}'
        self.wait_confimation()

    def send_reviews(self, reviews):
        bytes = self.make_header(TlvTypes.REVIEW_CHUNK, len(reviews))
        bytes += self.review_serializer.to_bytes(reviews)
        result = self.TCPHandler.send_all(bytes)
        assert result == len(bytes), f'Cannot send all bytes {result} != {len(bytes)}'
        self.wait_confimation()

    def send_lines(self, lines):
        bytes = self.make_header(TlvTypes.LINE_CHUNK, len(lines))
        bytes += self.line_serializer.to_bytes(lines)
        result = self.TCPHandler.send_all(bytes)
        assert result == len(bytes), f'Cannot send all bytes {result} != {len(bytes)}'
        self.wait_confimation()

    # TODO: maybe move to protocol.py
    def make_header(self, tlv_type, payload_len):
        raw_header = code_to_bytes(tlv_type)
        raw_header += make_msg_id()
        raw_header += int.to_bytes(payload_len, SIZE_LENGTH, "big") 
        return raw_header 

    def read_header(self):
        """
        Reads the Type, Message ID, and Length of TLV from self.TCPHandler and returns both.
        It reads a fixed amount of bytes (SIZE_CODE_MSG+SIZE_LENGTH)
        """
        _type_raw = self.TCPHandler.read(TlvTypes.SIZE_CODE_MSG)
        _type = struct.unpack('!i', _type_raw)[0]

        _mid_raw = self.TCPHandler.read(MSG_ID_LEN)
        _mid = msg_id_from_bytes(_mid_raw)

        _len_raw = self.TCPHandler.read(SIZE_LENGTH)
        _len = struct.unpack('!i', _len_raw)[0]

        return _type, _mid, _len

    def read(self):
        tlv_type, msg_id, tlv_len = self.read_header()

        if tlv_type in [TlvTypes.EOF, TlvTypes.ACK, TlvTypes.WAIT, TlvTypes.POLL]:
            return tlv_type, msg_id, None

        elif tlv_type == TlvTypes.BOOK_CHUNK:
            return TlvTypes.BOOK_CHUNK, msg_id, self.book_serializer.from_chunk(self.TCPHandler, header=False, n_chunks=tlv_len)

        elif tlv_type == TlvTypes.REVIEW_CHUNK:
            return TlvTypes.REVIEW_CHUNK, msg_id, self.review_serializer.from_chunk(self.TCPHandler, header=False, n_chunks=tlv_len)

        elif tlv_type == TlvTypes.LINE_CHUNK:
            return TlvTypes.LINE_CHUNK, msg_id, self.line_serializer.from_chunk(self.TCPHandler, header=False, n_chunks=tlv_len)

        else:
            raise UnexpectedType()

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
        self.TCPHandler.close()
        # cerrar la conexion
        return
