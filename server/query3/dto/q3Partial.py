import ast
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

    @classmethod
    def decode(cls, k: str, v: str):
        # TODO: mejorar!!
        authors, n, scoreAvg = ast.literal_eval(v)
        p = cls(
            title=k,
            authors=authors,
            n=n,
            scoreAvg=scoreAvg
        )
        return p

    def encode(self):
        # TODO: mejorar!!
        return str((self.authors, self.n, self.scoreAvg))

    def update(self, review: Review):
        avg = self.scoreAvg
        n = self.n

        self.scoreAvg = (avg*n + review.score)/(n+1)
        self.n += 1

    def merge(self, other):
        self.scoreAvg = (self.scoreAvg*self.n + other.scoreAvg*other.n)/(self.n + other.n)
        self.n += other.n
