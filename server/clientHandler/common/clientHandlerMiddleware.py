from utils.middleware.middleware import Middleware

class ClientHandlerMiddleware(Middleware):
    def __init__(self):
        super().__init__()
        
        # Declare queue to send books to QUERY1
        self.channel.queue_declare(queue='Q1-Books', durable=True)

        # Declare queue to send books to QUERY2
        self.channel.queue_declare(queue='Q2-Books', durable=True)

        # Declare exchange to send books to QUERY 3/4
        self.channel.exchange_declare(exchange='Q3-Books', exchange_type='direct')

    def send_booksQ1(self, bytes):
        self.send_msg(routing_key='Q1-Books', data=bytes, exchange='')

    def send_booksQ2(self, bytes):
        self.send_msg(routing_key='Q2-Books', data=bytes, exchange='')

    def send_booksQ3(self, bytes):
        self.send_msg(routing_key='', data=bytes, exchange='Q3-Books')

    def send_books_eof(self, bytes):
        self.send_msg(routing_key='Q1-Books', data=bytes, exchange='')
        self.send_msg(routing_key='Q2-Books', data=bytes, exchange='')
        self.send_msg(routing_key='', data=bytes, exchange='Q3-Books')
