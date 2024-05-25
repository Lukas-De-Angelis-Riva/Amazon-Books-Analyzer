from utils.protocol import TlvTypes, SIZE_LENGTH
from utils.protocol import integer_from_bytes


class Serializer:
    def read_t(self, reader):
        _type_raw = reader.read(TlvTypes.SIZE_CODE_MSG)
        _type = integer_from_bytes(_type_raw)
        return _type

    def read_l(self, reader):
        _len_raw = reader.read(SIZE_LENGTH)
        _len = integer_from_bytes(_len_raw)
        return _len

    def read_tl(self, reader):
        return self.read_t(reader), self.read_l(reader)

    def from_chunk(self, reader, header=True, n_chunks=None):
        if header:
            _, n_chunks = self.read_tl(reader)

        _list = []
        for i in range(n_chunks):
            _tlv_type, tlv_len = self.read_tl(reader)
            obj = self.from_bytes(reader, tlv_len)
            _list.append(obj)

        return _list

    def from_bytes(self, reader, obj_length):
        raw_dict = self.make_raw_dict()

        bytes_readed = 0
        while bytes_readed < obj_length:

            field_type = self.read_t(reader)
            bytes_readed += TlvTypes.SIZE_CODE_MSG

            length = self.read_l(reader)
            bytes_readed += SIZE_LENGTH

            raw_field = reader.read(length)
            bytes_readed += length

            if isinstance(raw_dict[field_type], list):
                raw_dict[field_type].append(raw_field)
            else:
                raw_dict[field_type] = raw_field
        return self.from_raw_dict(raw_dict)

    def make_raw_dict(self):
        raise RuntimeError("Must be redefined")

    def from_raw_dict(self, raw_dict):
        raise RuntimeError("Must be redefined")

    def to_bytes(self, chunk: list):
        raise RuntimeError("Must be redefined")
