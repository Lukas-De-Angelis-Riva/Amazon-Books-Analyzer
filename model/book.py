class Book:
    def __init__(self, title:str, authors:list, publisher:str, publishedDate:str, categories:list):
        self.title = title
        self.authors = authors
        self.publisher = publisher
        self.publishedDate = publishedDate
        self.categories = categories
    def __repr__(self):
        return f'Book(\"{self.title}\" - {self.authors})'
    def __str__(self):
        return f'Book(\"{self.title}\" - {self.authors})'