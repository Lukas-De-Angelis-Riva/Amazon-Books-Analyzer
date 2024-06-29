import struct
from model.review import Review

class Q3Partial:
    def __init__(self, title: str, authors: list, n: int = 0, scoreAvg: float = 0):
        self.title = title
        self.authors = authors
        self.n = n
        self.scoreAvg = scoreAvg

    def __repr__(self):
        return f'Q3Partial(Title:{self.title}, author: {self.authors}, n: {self.n}, scoreAvg: {self.scoreAvg})'

    def __str__(self):
        return f'Q3Partial(Title:{self.title}, author: {self.authors}, n: {self.n}, scoreAvg: {self.scoreAvg})'

    def copy(self):
        return Q3Partial(
            title=self.title,
            authors=self.authors.copy(),
            n=self.n,
            scoreAvg=self.scoreAvg
        )

    def key(self):
        return self.title

    @classmethod
    def decode(cls, reader):
        title_len = int.from_bytes(reader.read(1), byteorder='big')
        title = reader.read(title_len).decode('utf-8')

        n_auth = int.from_bytes(reader.read(1), byteorder='big')
        authors = []
        for _ in range(n_auth):
            author_len = int.from_bytes(reader.read(1), byteorder='big')
            author_i = reader.read(author_len).decode('utf-8')
            authors.append(author_i)

        n = int.from_bytes(reader.read(4), byteorder='big')
        avg = struct.unpack('!f', reader.read(4))[0]
        return cls(
            title,
            authors,
            n,
            avg
        )

    def encode(self):
        bytes_title = self.title.encode('utf-8')

        bytes_arr = b''
        bytes_arr += int.to_bytes(len(self.authors), length=1, byteorder='big')
        for author in self.authors:
            bytes_auth = author.encode('utf-8')
            bytes_arr += int.to_bytes(len(bytes_auth), length=1, byteorder='big')
            bytes_arr += bytes_auth

        bytes = b''
        bytes += int.to_bytes(len(bytes_title), length=1, byteorder='big')
        bytes += bytes_title

        bytes += bytes_arr

        bytes += int.to_bytes(self.n, length=4, byteorder='big')
        bytes += struct.pack("!f", self.scoreAvg)   # std_length=4
        return bytes

    def update(self, review: Review):
        avg = self.scoreAvg
        n = self.n

        self.scoreAvg = (avg*n + review.score)/(n+1)
        self.n += 1

    def merge(self, other):
        self.scoreAvg = (self.scoreAvg*self.n + other.scoreAvg*other.n)/(self.n + other.n)
        self.n += other.n
