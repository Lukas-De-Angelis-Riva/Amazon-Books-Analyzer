from utils.middleware.middleware import Middleware

class ClientHandlerMiddleware(Middleware):
    def __init__(self):
        super().__init__()
        
    def send_booksQ1(self, bytes):
        self.send_msg(routing_key='Q1-Books', data=bytes, exchange='')

    def send_booksQ2(self, bytes):
        self.send_msg(routing_key='Q2-Books', data=bytes, exchange='')

    def send_booksQ3(self, bytes):
        self.send_msg(routing_key='', data=bytes, exchange='Q3-Books')

    def send_booksQ5(self, bytes):
        self.send_msg(routing_key='', data=bytes, exchange='Q5-Books')

    def send_reviewsQ3(self, bytes):
        self.send_msg(routing_key='Q3-Reviews', data=bytes, exchange='')

    def send_reviewsQ5(self, bytes):
        self.send_msg(routing_key='Q5-Reviews', data=bytes, exchange='')
