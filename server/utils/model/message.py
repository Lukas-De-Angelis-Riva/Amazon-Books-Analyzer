from uuid import UUID, uuid4

import pickle
import enum


UUID_LEN = 16  # uuid4() yields 16 bytes
TYPE_LEN = 1

# HEADER OFFSETS
MSG_ID = 0
MSG_CLI_ID = MSG_ID + UUID_LEN
MSG_TYPE = MSG_CLI_ID + UUID_LEN
MSG_ARGS_LEN = MSG_TYPE + TYPE_LEN
MSG_ARGS = MSG_ARGS_LEN + 4


class MessageType(enum.Enum):
    DATA = b"\x00"
    EOF = b"\x01"
    ACK = b"\x02"
    NACK = b"\x03"


class Message():

    def __init__(self, client_id: UUID, type: MessageType, data: bytes, args: dict = {}, id = None):
        self.ID = id if id else uuid4()  # random 16 bytes UUID

        if not isinstance(client_id, UUID):
            raise TypeError("`client_id` must be of type uuid.UUID")
        self.client_id = client_id

        if not isinstance(type, MessageType):
            raise TypeError("`type` must be of type MessageType")
        self.type = type

        if not isinstance(data, bytes):
            raise TypeError("`data` must be of type bytes")
        self.data = data

        if isinstance(args, dict) and len(args) > 0:
            self.args = args
        else:
            self.args = {}

    def to_bytes(self) -> bytes:
        # header
        raw = self.ID.bytes
        raw += self.client_id.bytes
        raw += self.type.value

        # metadata + body
        raw_args = b""

        if self.args:
            raw_args = pickle.dumps(self.args)

        raw += len(raw_args).to_bytes(length=4, byteorder='big')
        raw += raw_args
        raw += self.data
        return raw

    @classmethod
    def from_bytes(cls, raw):
        ID = UUID(bytes=raw[MSG_ID: MSG_CLI_ID])
        client_id = UUID(bytes=raw[MSG_CLI_ID: MSG_TYPE])
        type = MessageType(value=raw[MSG_TYPE: MSG_ARGS_LEN])

        args_len = int.from_bytes(bytes=raw[MSG_ARGS_LEN: MSG_ARGS], byteorder='big')

        if args_len > 0:
            args = pickle.loads(raw[MSG_ARGS: MSG_ARGS + args_len])
        else:
            args = {}
        data = raw[MSG_ARGS + args_len:]

        m = cls(
            client_id=client_id,
            type=type,
            args=args,
            data=data,
        )
        m.ID = ID
        return m
