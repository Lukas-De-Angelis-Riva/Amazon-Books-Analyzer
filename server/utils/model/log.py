from uuid import UUID
import logging
import enum
import ast
import io
import os


class ShortLine(Exception):
    pass


class LogLineType(enum.Enum):
    BEGIN = b"\x01"
    COMMIT = b"\x02"
    WRITE = b"\x03"
    WRITE_METADATA = b"\x04"


class WriteMetadataLine():

    REPR = 'META'

    def __init__(self, key: str, old_value, encoder=None):
        self.type = LogLineType.WRITE_METADATA
        self.key = key
        self.old_value = old_value
        self.encoder = encoder

    def encode(self):
        _b = self.encoder(self.old_value)
        _key = self.key.encode('utf-8')

        b = b''
        b += self.type.value
        b += int.to_bytes(len(_key), length=1, byteorder='big')
        b += _key
        b += int.to_bytes(len(_b), length=1, byteorder='big')
        b += _b
        return b

    @classmethod
    def decode(cls, reader, decoder, header=True):
        if header:
            _r = reader.read(1)
            if len(_r) != 1:
                raise ShortLine
            ltype = LogLineType(_r)
            if ltype != LogLineType.WRITE_METADATA:
                raise ShortLine

        _r = reader.read(1)
        if len(_r) != 1:
            raise ShortLine
        key_len = int.from_bytes(_r, byteorder='big')

        _r = reader.read(key_len)
        if len(_r) != key_len:
            raise ShortLine
        key = _r.decode('utf-8')

        _r = reader.read(1)
        if len(_r) != 1:
            raise ShortLine
        v_len = int.from_bytes(_r, byteorder='big')

        _r = reader.read(v_len)
        if len(_r) != v_len:
            raise ShortLine
        v = decoder(key, _r)
        return cls(
            key,
            v
        )

    def to_line(self):
        return f'{WriteMetadataLine.REPR};{self.key};{str(self.old_value)}\n'

    @classmethod
    def from_line(cls, line: str):
        splitted = line.split(';')
        if splitted[0] != WriteMetadataLine.REPR:
            raise TypeError(f"Line '{line}' is not of type {WriteMetadataLine.REPR}.")

        key = splitted[1]
        # TODO: CHANGE THIS!
        old_value = ast.literal_eval(splitted[2])
        return cls(
            key,
            old_value
        )


class WriteLine():

    REPR = 'WRITE'

    def __init__(self, key: str, old_value):
        self.type = LogLineType.WRITE
        self.key = key
        self.old_value = old_value

    def encode(self):
        _b = self.old_value.encode()
        b = b''
        b += self.type.value
        b += int.to_bytes(len(_b), length=2, byteorder='big')
        b += _b
        return b

    @classmethod
    def decode(cls, reader, decoder, header=True):
        if header:
            _r = reader.read(1)
            if len(_r) != 1:
                raise ShortLine
            ltype = LogLineType(_r)
            if ltype != LogLineType.WRITE:
                raise ShortLine

        _r = reader.read(2)
        if len(_r) != 2:
            raise ShortLine
        encode_len = int.from_bytes(_r, byteorder='big')

        _b = reader.read(encode_len)
        if len(_b) != encode_len:
            raise ShortLine

        old_value = decoder(io.BytesIO(_b))

        return cls(
            old_value.key(),
            old_value
        )

    def to_line(self):
        return f'{WriteLine.REPR};{self.key};{self.old_value}\n'

    @classmethod
    def from_line(cls, line: str):
        splitted = line.split(';')
        if splitted[0] != WriteLine.REPR:
            raise TypeError(f"Line '{line}' is not of type {WriteLine.REPR}.")

        key = splitted[1]
        old_value = splitted[2]
        return cls(
            key,
            old_value
        )


class BeginLine():

    REPR = 'BEGIN'

    def __init__(self, chunk_id: UUID, worker_id=None):
        self.type = LogLineType.BEGIN
        self.chunk_id = chunk_id
        self.worker_id = worker_id if worker_id else 0

    def encode(self):
        b = b''
        b += self.type.value
        b += self.chunk_id.bytes
        b += int.to_bytes(self.worker_id, length=1, byteorder='big')
        return b

    @classmethod
    def decode(cls, reader, header=True):
        if header:
            _r = reader.read(1)
            if len(_r) != 1:
                raise ShortLine
            ltype = LogLineType(_r)
            if ltype != LogLineType.BEGIN:
                raise ShortLine

        _r = reader.read(16)
        if len(_r) != 16:
            raise ShortLine
        luuid = UUID(bytes=_r)

        _r = reader.read(1)
        if len(_r) != 1:
            raise ShortLine
        lworker_id = int.from_bytes(_r, byteorder='big')

        return cls(
            luuid,
            lworker_id,
        )

    def to_line(self):
        if self.worker_id:
            return f'{BeginLine.REPR};{str(self.chunk_id)};{self.worker_id}\n'
        else:
            return f'{BeginLine.REPR};{str(self.chunk_id)}\n'

    @classmethod
    def from_line(cls, line: str):
        splitted = line.split(';')
        if splitted[0] != BeginLine.REPR:
            raise TypeError(f"Line '{line}' is not of type {BeginLine.REPR}.")

        chunk_id = UUID(splitted[1])
        if len(splitted) > 2:
            worker_id = splitted[2]
            return cls(chunk_id, worker_id)
        else:
            return cls(chunk_id)


class CommitLine():

    REPR = 'COMMIT'

    def __init__(self, chunk_id: UUID, worker_id=None):
        self.type = LogLineType.COMMIT
        self.chunk_id = chunk_id
        self.worker_id = worker_id if worker_id else 0

    def encode(self):
        b = b''
        b += self.type.value
        b += self.chunk_id.bytes
        b += int.to_bytes(self.worker_id, length=1, byteorder='big')
        return b

    @classmethod
    def decode(cls, reader, header=True):
        if header:
            _r = reader.read(1)
            if len(_r) != 1:
                raise ShortLine
            ltype = LogLineType(_r)

            if ltype != LogLineType.COMMIT:
                raise ShortLine

        _r = reader.read(16)
        if len(_r) != 16:
            raise ShortLine
        luuid = UUID(bytes=_r)

        _r = reader.read(1)
        if len(_r) != 1:
            raise ShortLine
        lworker_id = int.from_bytes(_r, byteorder='big')

        return cls(
            luuid,
            lworker_id,
        )

    def to_line(self):
        if self.worker_id:
            return f'{CommitLine.REPR};{str(self.chunk_id)};{self.worker_id}\n'
        else:
            return f'{CommitLine.REPR};{str(self.chunk_id)}\n'

    @classmethod
    def from_line(cls, line: str):
        splitted = line.split(';')
        if splitted[0] != CommitLine.REPR:
            raise TypeError(f"Line '{line}' is not of type {CommitLine.REPR}.")

        chunk_id = UUID(splitted[1])
        if len(splitted) > 2:
            worker_id = splitted[2]
            return cls(chunk_id, worker_id)
        else:
            return cls(chunk_id)


class LogFactory():
    @classmethod
    def from_line(cls, line: str):
        splitted = line.split(';')
        if len(splitted) == 0:
            return None
        if splitted[0] == WriteLine.REPR:
            return WriteLine.from_line(line)
        elif splitted[0] == WriteMetadataLine.REPR:
            return WriteMetadataLine.from_line(line)
        elif splitted[0] == BeginLine.REPR:
            return BeginLine.from_line(line)
        elif splitted[0] == CommitLine.REPR:
            return CommitLine.from_line(line)
        return None

    @classmethod
    def from_lines(cls, lines: list):
        if len(lines) == 0:
            # No lines written
            return []
        last_line = lines[-1]
        if last_line[-1] != '\n':
            # Bad writting
            del lines[-1]

        return [
            cls.from_line(line.strip())
            for line in lines
        ]

    @classmethod
    def from_bytes(cls, reader, decoder, meta_decoder):
        end = reader.seek(0, os.SEEK_END)
        reader.seek(0, os.SEEK_SET)
        lines = []
        while reader.tell() < end:
            try:
                _r = reader.read(1)
                if len(_r) != 1:
                    raise ShortLine
                ltype = LogLineType(_r)
                if ltype == LogLineType.WRITE:
                    line = WriteLine.decode(reader, decoder, header=False)
                    lines.append(line)
                elif ltype == LogLineType.WRITE_METADATA:
                    line = WriteMetadataLine.decode(reader, meta_decoder, header=False)
                    lines.append(line)
                elif ltype == LogLineType.COMMIT:
                    line = CommitLine.decode(reader, header=False)
                    lines.append(line)
                elif ltype == LogLineType.BEGIN:
                    line = BeginLine.decode(reader, header=False)
                    lines.append(line)
            except ShortLine:
                logging.debug(f"action: parsing_log | result: bad_line | truncating_to: {len(lines)}")
                break
            except ValueError as verror:
                logging.error(f"action: parsing_log | result: error | traceback: {str(verror) or repr(verror)}")
                break
        return lines
