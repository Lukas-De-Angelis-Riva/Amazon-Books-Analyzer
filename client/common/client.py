from utils.protocolHandler import ProtocolHandler
from utils.TCPhandler import SocketBroken
from model.book import Book

import logging
import socket
import csv
import os

TITLE = 0
AUTHORS = 2
PUBLISHER = 5
PUBLISHED_DATE = 6
CATEGORIES = 8

class Client:
    def __init__(self, ip, port, book_file_path, chunk_size_book):
        # Initialize server socket
        self.ip = ip
        self.port = port
        self.book_file_path = book_file_path
        self.chunk_size_book = chunk_size_book
        self.socket = None
        self.handler = None
        self.signal_received = False

    def run(self):
        logging.info(f'action: running client')
        # Read airports.csv and send to the system.
        self.connect(self.ip, self.port)
        self.send_books()
        
        self.disconnect()
        logging.info(f'action: closing client')

    def connect(self, ip, port):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((ip, port))
        self.protocolHandler = ProtocolHandler(self.socket)

    def disconnect(self):
        self.socket.close()

    # Title,description,authors,image,previewLink,publisher,publishedDate,infoLink,categories,ratingsCount
    def read_book_line(self, line):
        r = csv.reader([line], )
        _book = list(r)[0]
        book = Book(
            title = _book[TITLE],
            authors = [author.strip(" '[]") for author in _book[AUTHORS].split(',')],
            publisher = _book[PUBLISHER],
            publishedDate = _book[PUBLISHED_DATE].split("-")[0].strip("*?"),
            categories = [category.strip(" '[]") for category in _book[CATEGORIES].split(',')],
        )
        '''
        book.title = book.title if len(book.title) > 0 else "-"
        book.authors = book.authors if len(book.authors) > 0 else ["-"]
        book.publisher = book.publisher if len(book.publisher) != 0 else "-"
        book.publishedDate = book.publishedDate if len(book.publishedDate) != 0 else "-"
        book.categories = book.categories if len(book.categories) > 0 else ["-"]
        '''
        if len(book.title) == 0 or \
            len(book.authors) == 0 or \
            len(book.publisher) == 0  or \
            len(book.publishedDate) == 0 or \
            len(book.categories) == 0:
            return None
        return book

    def send_books(self):
        self.send_file(self.book_file_path,
                       self.read_book_line,
                       self.chunk_size_book,
                       self.protocolHandler.send_books,
                       self.protocolHandler.send_book_eof,
                       )

    def send_file(self, path, read_line, chunk_size, send_message, send_eof):
        logging.info(f'action: send file | result: in_progress | path: {path}')
        try:
            file_size = os.path.getsize(path)
            with open(path, mode ='r') as file:
                file.readline()  # skip the headers
                batch = []
                i = 0
                while line := file.readline():
                    element = read_line(line)
                    if element != None:
                        logging.info(f'action: read_element | result: success | element: {element}')
                        batch.append(element)
                        if len(batch) == chunk_size:
                            send_message(batch)
                            batch = []
                            logging.info('action: read {} | progress: {:.2f}%'.format(
                                path, 100*(file.tell())/(file_size)
                            ))
                    else:
                        logging.info(f'action: read_element | result: discard')
                    i+=1

                if batch:
                    send_message(batch)
                    logging.info('action: read {} | progress: {:.2f}%'.format(
                        path, 100*(file.tell())/(file_size)
                    ))
                send_eof()
        except (SocketBroken,OSError) as e:
            if not self.signal_received:
                logging.error(f'action: send file | result: fail | error: {e}')
        else:
            logging.info(f'action: send file | result: success | path: {path}')