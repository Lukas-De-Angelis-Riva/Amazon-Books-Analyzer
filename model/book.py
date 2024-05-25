class Book:
    def __init__(self, title: str = None, authors: list = None, publisher: str = None, publishedDate: str = None, categories: list = None):
        self.title = title
        self.authors = authors
        self.publisher = publisher
        self.publishedDate = publishedDate
        self.categories = categories

    def __repr__(self):
        return f'Book(\"{self.title}\" - {self.authors})'

    def __str__(self):
        return f'Book(\"{self.title}\" - {self.authors})'
