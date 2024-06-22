from uuid import UUID
import enum


class LogLineType(enum.Enum):
    BEGIN = b"\x00"
    COMMIT = b"\x01"
    WRITE = b"\x02"
    WRITE_METADATA = b"\x03"


class WriteMetadataLine():

    REPR = 'META'

    def __init__(self, key: str, old_value):
        self.type = LogLineType.WRITE_METADATA
        self.key = key
        self.old_value = old_value

    def to_line(self):
        return f'{WriteMetadataLine.REPR};{self.key};{str(self.old_value)}\n'

    @classmethod
    def from_line(cls, line: str):
        splitted = line.split(';')
        if splitted[0] != WriteMetadataLine.REPR:
            raise TypeError(f"Line '{line}' is not of type {WriteMetadataLine.REPR}.")

        key = splitted[1]
        old_value = int(splitted[2])
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
        self.worker_id = worker_id

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
        self.worker_id = worker_id

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
