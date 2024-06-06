import pickle
import struct 

class Token():
    peer_id = -1
    metadata = {}

    def __init__(self, peer_id, metadata):
        self.peer_id = peer_id
        self.metadata = metadata

    def to_bytes(self) -> bytes:
        raw = struct.pack("!i", self.peer_id)
        raw += pickle.dump(metadata)
        return raw

    @classmethod
    def from_bytes(self, raw) -> None:
        self.peer_id = struct.unpack("!i", raw[:4])
        self.metadata = pickle.loads(raw[4:])


class EOF():
    total = 0

    def __init__(self, total):
        self.total=total
    
    def to_bytes(self) -> bytes:
        return struct.pack("!i", self.total)

    @classmethod
    def from_bytes(self, raw: bytes) -> None:
        self.total = struct.unpack("!i", raw)


