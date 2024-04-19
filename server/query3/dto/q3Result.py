class Q3Result:
    def __init__(self, title: str, authors: list):
        self.title = title
        self.authors = authors

    def __repr__(self):
        return f'Q3Result(Title:{self.title}, authors: {self.authors}'

    def __str__(self):
        return f'Q3Result(Title:{self.title}, authors: {self.authors}'