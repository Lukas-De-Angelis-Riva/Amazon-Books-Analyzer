class Review:
    def __init__(self, id, title, score, text):
        self.id = id
        self.title = title
        self.score = score
        self.text = text
    def __repr__(self):
        return f'Review({self.id}, \"{self.title}\", {self.score})'
    def __str__(self):
        return f'Review({self.id}, \"{self.title}\", {self.score})'