from utils.worker import Worker
from utils.middleware.middlewareEQ import MiddlewareEQ
from utils.serializer.bookQ3serializer import BookQ3Serializer

class BookQ3Handler(Worker):
    def __init__(self, books, minimun_date,maximun_date):
        middleware = MiddlewareEQ(exchange='books-Q3/4',
                                  tag='',
                                  out_queue_name=None)
        super().__init__(middleware=middleware,
                         in_serializer=BookQ3Serializer(),
                         out_serializer=None,
                         peers=0,
                         chunk_size=0)
        self.books = books
        self.minimun_date = minimun_date
        self.maximun_date = maximun_date

    def work(self, input):
        book = input
        published_date = book.published_date

        if published_date <= self.maximun_date and published_date >= self.minimun_date:
            self.books[book.title] = (book.authors,book.publishedDate,0)

    def recv_eof(self, eof):
        return