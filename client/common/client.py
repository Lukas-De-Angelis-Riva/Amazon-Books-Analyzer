from utils.protocolHandler import ProtocolHandler
from utils.TCPhandler import SocketBroken
from model.book import Book
from model.review import Review

import logging
import socket
import signal
import uuid
import time
import csv
import os

from alive_progress import alive_bar

TITLE = 0
AUTHORS = 2
PUBLISHER = 5
PUBLISHED_DATE = 6
CATEGORIES = 8

REVIEW_ID = 0
REVIEW_TITLE = 1
REVIEW_SCORE = 6
REVIEW_TEXT = 9

MAX_TIME_SLEEP = 8      # seconds
MIN_TIME_SLEEP = 1      # seconds
TIME_SLEEP_SCALE = 2    # 2 * t


class Client:
    def __init__(self, config_params):
        # Initialize server socket
        self.config = config_params
        self.socket = None
        self.handler = None
        self.query_sizes = {'Q1': 0, 'Q2': 0, 'Q3': 0, 'Q4': 0, 'Q5': 0}
        self.id = uuid.uuid4()

        signal.signal(signal.SIGTERM, self.__handle_signal)
        self.signal_received = False

    def run(self):
        logging.info(f'action: running client | CLIENT-ID: {self.id}')
        # Read books.csv and send to the system.

        if not os.path.isfile(self.config["book_file_path"]):
            logging.error(f'action: run | result: fail | error: {self.config["book_file_path"]} does not exists.')
            return
        if not os.path.isfile(self.config["review_file_path"]):
            logging.error(f'action: run | result: fail | error: {self.config["review_file_path"]} does not exists.')
            return

        try:
            self.connect(self.config["ip"], self.config["port"])
        except Exception as e:
            logging.error(f'action: connect | result: fail | error: {str(e)}')
            return

        ok = self.handshake()
        if not ok:
            return

        self.send_books()
        self.send_reviews()

        if self.signal_received:
            return
        self.disconnect()

        # Poll results for all querys
        self.connect(self.config["results_ip"], self.config["results_port"])
        logging.info('action: poll_results | result: in_progress')
        self.poll_results()
        if self.signal_received:
            return
        logging.info('action: poll_results | result: success | nQ1: {} | nQ2: {} | nQ3: {} | nQ4: {} | nQ5: {}'.format(
            self.query_sizes['Q1'], self.query_sizes['Q2'], self.query_sizes['Q3'], self.query_sizes['Q4'], self.query_sizes['Q5']
        ))
        self.disconnect()

        logging.info('action: closing client')

    def connect(self, ip, port):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((ip, port))
        self.protocolHandler = ProtocolHandler(self.socket)

    def disconnect(self):
        self.socket.close()

    def __handle_signal(self, signum, frame):
        logging.debug(f'action: stop_client | result: in_progress | signal {signum}')
        self.disconnect()
        self.signal_received = True
        logging.debug('action: stop_client | result: success')

    def handshake(self):
        try:
            logging.debug('action: handshake | result: in_progress')
            self.protocolHandler.handshake(self.id)
            logging.debug('action: handshake | result: success')
        except (SocketBroken, OSError) as e:
            logging.error(f'action: handshake | result: fail | error: {str(e) or repr(e)}')
            return False
        return True

    def send_books(self):
        self.send_file(self.config["book_file_path"],
                       self.read_book_line,
                       self.config["chunk_size_book"],
                       self.protocolHandler.send_books,
                       self.protocolHandler.send_book_eof,
                       )

    def send_reviews(self):
        self.send_file(self.config["review_file_path"],
                       self.read_review_line,
                       self.config["chunk_size_review"],
                       self.protocolHandler.send_reviews,
                       self.protocolHandler.send_review_eof,
                       )

    def send_file(self, path, read_line, chunk_size, send_message, send_eof):
        logging.info(f'action: send file | result: in_progress | path: {path}')

        try:
            file_size = os.path.getsize(path)
            with open(path, mode='r') as file, alive_bar(100, manual=True, force_tty=True) as bar:
                file.readline()  # skip the headers
                batch = []
                i = 0
                while line := file.readline():
                    bar(float(file.tell() / file_size))
                    element = read_line(line)
                    if element is not None:
                        logging.debug(f'action: read_element | result: success | element: {element}')
                        batch.append(element)
                        if len(batch) == chunk_size:
                            send_message(batch)
                            batch = []
                    else:
                        logging.debug('action: read_element | result: discard')
                    i += 1

                if batch:
                    send_message(batch)
                    bar(1.0)
                send_eof()
        except (SocketBroken, OSError) as e:
            if not self.signal_received:
                logging.error(f'action: send file | result: fail | error: {e}')
        else:
            logging.info(f'action: send file | result: success | path: {path}')

    def save_results(self, results):
        with open(self.config['results_path'], 'a') as file:
            for result in results:
                file.write(result)
                file.write('\n')
                self.query_sizes[result[:2]] += 1
                logging.info(f'result: {result}')

    def poll_results(self):
        try:
            keep_running = True
            t_sleep = MIN_TIME_SLEEP
            while keep_running:
                logging.debug('action: polling | result: in_progress')
                t, value = self.protocolHandler.poll_results()
                if self.protocolHandler.is_result_wait(t):
                    logging.debug('action: polling | result: wait')
                    time.sleep(t_sleep)
                    t_sleep = min(TIME_SLEEP_SCALE*t_sleep, MAX_TIME_SLEEP)
                elif self.protocolHandler.is_result_eof(t):
                    logging.debug('action: polling | result: eof')
                    keep_running = False
                elif self.protocolHandler.is_results(t):
                    logging.debug(f'action: polling | result: succes | len(results): {len(value)}')
                    t_sleep = max(t_sleep/TIME_SLEEP_SCALE, MIN_TIME_SLEEP)
                    self.save_results(value)
                else:
                    logging.error(f'action: polling | result: fail | unknown_type: {t}')
        except (Exception, KeyboardInterrupt) as e:
            if not self.signal_received:
                logging.error(f'action: polling | result: fail | error: {e}')
        else:
            logging.debug('action: polling | result: success')

    def read_book_line(self, line):
        r = csv.reader([line], )
        _book = list(r)[0]
        book = Book(
            title=_book[TITLE],
            authors=[author.strip(" '[]") for author in _book[AUTHORS].split(',') if author.strip(" '[]") != ""],
            publisher=_book[PUBLISHER],
            publishedDate=_book[PUBLISHED_DATE].split("-")[0].strip("*?"),
            categories=[category.strip(" '[]") for category in _book[CATEGORIES].split(',')],
        )

        if len(book.title) == 0 or \
           len(book.authors) == 0 or \
           len(book.publisher) == 0 or \
           len(book.publishedDate) == 0 or \
           len(book.categories) == 0:
            return None
        return book

    def read_review_line(self, line):
        r = csv.reader([line], )
        _review = list(r)[0]
        review = Review(
            id=_review[REVIEW_ID],
            title=_review[REVIEW_TITLE],
            score=float(_review[REVIEW_SCORE]),
            text=_review[REVIEW_TEXT],
        )

        if len(review.title) == 0 or \
           len(review.text) == 0:
            return None
        return review
