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

    def update(self, review: Review):
        avg = self.sentimentAvg
        n = self.n
        _sentiment = sentiment(review.text)

        self.sentimentAvg = (avg*n + _sentiment)/(n+1)
        self.n += 1

    def merge(self, other):
        self.sentimentAvg = (self.sentimentAvg*self.n + other.sentimentAvg*other.n)/(self.n + other.n)
        self.n += other.n
