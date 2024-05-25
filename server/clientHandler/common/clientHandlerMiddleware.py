from utils.middleware.middleware import Middleware

class ClientHandlerMiddleware(Middleware):
    def __init__(self):
        super().__init__()

    def send_booksQ1(self, bytes):
        self.produce(bytes, 'Q1-Books')

    def send_booksQ2(self, bytes):
        self.produce(bytes, 'Q2-Books')

    def send_booksQ3(self, bytes):
        self.publish(bytes, 'Q3-Books', '')

    def send_booksQ5(self, bytes):
        self.publish(bytes, 'Q5-Books', '')

    def send_reviewsQ3(self, bytes):
        self.produce(bytes, 'Q3-Reviews')

    def send_reviewsQ5(self, bytes):
        self.produce(bytes, 'Q5-Reviews')
