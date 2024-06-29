import struct
from model.review import Review
from textblob import TextBlob


def sentiment(text):
    blob = TextBlob(text)
    return blob.sentiment.polarity


class Q5Partial:
    def __init__(self, title: str, n: int = 0, sentimentAvg: float = 0):
        self.title = title
        self.n = n
        self.sentimentAvg = sentimentAvg

    def __repr__(self):
        return f'Q5Partial(Title:{self.title} | n: {self.n} | avg: {self.sentimentAvg})'

    def __str__(self):
        return f'Q5Partial(Title:{self.title} | n: {self.n} | avg: {self.sentimentAvg})'

    def copy(self):
        return Q5Partial(
            title=self.title,
            n=self.n,
            sentimentAvg=self.sentimentAvg
        )

    def key(self):
        return self.title

    @classmethod
    def decode(cls, reader):
        title_len = int.from_bytes(reader.read(1), byteorder='big')
        title = reader.read(title_len).decode('utf-8')

        n = int.from_bytes(reader.read(4), byteorder='big')
        avg = struct.unpack('!f', reader.read(4))[0]
        return cls(
            title,
            n,
            avg
        )

    def encode(self):
        bytes_title = self.title.encode('utf-8')

        bytes = b''
        bytes += int.to_bytes(len(bytes_title), length=1, byteorder='big')
        bytes += bytes_title

        bytes += int.to_bytes(self.n, length=4, byteorder='big')
        bytes += struct.pack("!f", self.sentimentAvg)   # std_length=4
        return bytes

    def update(self, review: Review):
        avg = self.sentimentAvg
        n = self.n
        _sentiment = sentiment(review.text)

        self.sentimentAvg = (avg*n + _sentiment)/(n+1)
        self.n += 1

    def merge(self, other):
        self.sentimentAvg = (self.sentimentAvg*self.n + other.sentimentAvg*other.n)/(self.n + other.n)
        self.n += other.n
