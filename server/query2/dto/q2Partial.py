import ast
from model.book import Book


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

    @classmethod
    def decode(cls, k: str, v: str):
        p = cls(
            author=k,
            # It's safe... it only allows parsing of Python literals
            decades=ast.literal_eval(v)
        )
        return p

    def encode(self):
        return str(list(self.decades))

    def update(self, book: Book):
        decade = 10 * (int(book.publishedDate)//10)
        self.decades.add(decade)

    def merge(self, other):
        self.decades |= other.decades
