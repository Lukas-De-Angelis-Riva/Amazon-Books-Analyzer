from typing import Self

import pickle


class Token():

    def __init__(self, leader, metadata):
        self.leader = leader
        self.metadata = metadata

    def to_bytes(self) -> bytes:
        raw = self.leader.to_bytes(length=4)
        raw += pickle.dump(self.metadata)
        return raw

    @classmethod
    def from_bytes(cls, raw) -> Self:
        leader = int.from_bytes(raw[:4])
        metadata = pickle.loads(raw[4:])
        return cls(leader, metadata)
