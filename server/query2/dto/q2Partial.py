class Q2Partial:
    def __init__(self, author: str, decades: list):
        self.author = author
        self.decades = set(decades)

    def __repr__(self):
        return f'Q2Partial(Author:{self.author} | len(decades): {len(self.decades)})'

    def __str__(self):
        return f'Q2Partial(Author:{self.author} | len(decades): {len(self.decades)})'

    def copy(self):
        return Q2Partial(
            author=self.author,
            decades=list(self.decades)
        )

    def key(self):
        return self.author

    @classmethod
    def decode(cls, reader):
        author_len = int.from_bytes(reader.read(1), byteorder='big')
        author = reader.read(author_len).decode('utf-8')
        dec_len = int.from_bytes(reader.read(1), byteorder='big')

        dec_n = dec_len // 2
        decades = []
        for _ in range(dec_n):
            i = int.from_bytes(reader.read(2), byteorder='big')
            decades.append(i)

        return cls(author, decades)

    def encode(self):
        bytes_aut = self.author.encode('utf-8')
        bytes_arr = b''
        for i in self.decades:
            bytes_arr += int.to_bytes(i, length=2, byteorder='big')
        bytes = b''
        bytes += int.to_bytes(len(bytes_aut), length=1, byteorder='big')
        bytes += bytes_aut
        bytes += int.to_bytes(len(bytes_arr), length=1, byteorder='big')
        bytes += bytes_arr
        return bytes

    def update(self, book):
        decade = 10 * (int(book.publishedDate)//10)
        self.decades.add(decade)

    def merge(self, other):
        self.decades |= other.decades
