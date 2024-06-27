from utils.protocolHandler import ProtocolHandler
from utils.TCPhandler import SocketBroken
from model.book import Book
from model.review import Review

from utils.protocol import TlvTypes
from utils.serializer.bookSerializer import BookSerializer
from utils.serializer.reviewSerializer import ReviewSerializer

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

N_RETRIES = 10
SOCK_TIMEOUT = 5


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
        self.curr_results_page = 0

    def run(self):
        logging.info(f'action: running client | CLIENT-ID: {self.id}')
        # Read books.csv and send to the system.

        if not os.path.isfile(self.config["book_file_path"]):
            logging.error(f'action: run | result: fail | error: {self.config["book_file_path"]} does not exists.')
            return
        if not os.path.isfile(self.config["review_file_path"]):
            logging.error(f'action: run | result: fail | error: {self.config["review_file_path"]} does not exists.')
            return

        if not self.connect(self.config["ip"], self.config["port"], timeout=SOCK_TIMEOUT):
            return

        if not self.send_books():
            return

        if not self.send_reviews():
            return

        if self.signal_received:
            return

        if not self.connect(self.config["results_ip"], self.config["results_port"], timeout=SOCK_TIMEOUT, tries=1+N_RETRIES):
            return

        ok = self.get_results()
        if not ok or self.signal_received:
            return

        logging.info('action: poll_results | result: success | nQ1: {} | nQ2: {} | nQ3: {} | nQ4: {} | nQ5: {}'.format(
            self.query_sizes['Q1'],
            self.query_sizes['Q2'],
            self.query_sizes['Q3'],
            self.query_sizes['Q4'],
            self.query_sizes['Q5']
        ))
        self.disconnect()

        logging.info('action: closing client')

    def connect(self, ip, port, timeout=None, tries=1):
        if self.socket is not None:
            self.disconnect()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(timeout)
        sleep = MIN_TIME_SLEEP
        err = None

        for _ in range(tries):
            try:
                self.socket.connect((ip, port))
                self.protocolHandler = ProtocolHandler(self.socket)
                return self.handshake(tries=tries)
            except socket.error as e:
                err = e
                time.sleep(sleep)
                sleep = min(sleep*TIME_SLEEP_SCALE, MAX_TIME_SLEEP)
                continue
        logging.error(f'action: connect | result: fail | error: {str(err) or repr(err)} | tries: {tries}')
        # self.socket.settimeout(None)
        return False

    def disconnect(self):
        self.protocolHandler.close()
        self.protocolHandler = None
        self.socket = None

    def __handle_signal(self, signum, frame):
        logging.debug(f'action: stop_client | result: in_progress | signal {signum}')
        self.disconnect()
        self.signal_received = True
        logging.debug('action: stop_client | result: success')

    def handshake(self, tries=1):
        sleep = MIN_TIME_SLEEP
        for i in range(tries):
            try:
                logging.debug('action: handshake | result: in_progress')
                self.protocolHandler.handshake(self.id)
                logging.debug('action: handshake | result: success')
                return True
            except socket.error:
                time.sleep(sleep)
                sleep = min(sleep*TIME_SLEEP_SCALE, MAX_TIME_SLEEP)
                continue
            except (SocketBroken, OSError) as e:
                logging.error(f'action: handshake | result: fail | error: {str(e) or repr(e)} | tries: {i}')
                return False

        return False

    def __send_book_eof(self):
        self.protocolHandler.send_book_eof()

    def __send_review_eof(self):
        self.protocolHandler.send_review_eof()

    def send_books(self):
        return self.send_file(self.config["book_file_path"],
                              self.read_book_line,
                              self.config["chunk_size_book"],
                              TlvTypes.BOOK_CHUNK,
                              BookSerializer(),
                              self.__send_book_eof
                              )

    def send_reviews(self):
        return self.send_file(self.config["review_file_path"],
                              self.read_review_line,
                              self.config["chunk_size_review"],
                              TlvTypes.REVIEW_CHUNK,
                              ReviewSerializer(),
                              self.__send_review_eof
                              )

    def send_file(self, path, read_line, chunk_size, msg_type, serializer, send_eof, pos=0):
        logging.info(f'action: send file | result: in_progress | path: {path}')

        try:
            file_size = os.path.getsize(path)
            with open(path, mode='r') as file, alive_bar(100, manual=True, force_tty=True) as bar:
                # file.seek(pos)
                file.readline()  # skip the headers
                batch = []
                line = file.readline()
                i = 1

                while line:
                    bar(float(file.tell() / file_size))
                    element = read_line(line)
                    line = file.readline()  # read next line

                    if element is None:
                        logging.debug('action: read_element | result: discard')
                        continue
                    logging.debug(f'action: read_element | result: success | element: {element}')
                    batch.append(element)
                    i += 1
                    sleep = MIN_TIME_SLEEP

                    for tries in range(2 + N_RETRIES):
                        try:
                            if tries > N_RETRIES:
                                logging.error(f'action: send_file | file: {path} | error: server not available')
                                return False

                            if tries:
                                logging.info("action: retry send_batch | result: in_progress")

                            # send also if next line is null
                            if len(batch) == chunk_size or not line:
                                self.protocolHandler.send_batch(batch, msg_type, serializer)
                                batch = []
                                logging.debug('action: send_batch | result: success')
                            break
                        except (socket.error, SocketBroken) as e:
                            logging.error(f'action: socket_error | error: {str(e) or repr(e)}')
                            # retry connection
                            if not self.connect(self.config["ip"], self.config["port"], timeout=SOCK_TIMEOUT, tries=1+N_RETRIES):
                                return False
                        # if send has timeouted (includes if ack not received)
                        # TODO: make ProtocolHandler exception class
                        except socket.timeout:
                            logging.error('action: timeout_error')
                            time.sleep(sleep)
                            sleep *= min(sleep*TIME_SLEEP_SCALE, MAX_TIME_SLEEP)

                send_eof()
                bar(1.0)
                return True

        except OSError as e:
            if not self.signal_received:
                logging.error(f'action: send file | result: fail | error: {str(e) or repr(e)}')
            return False

    def save_results(self, results):
        with open(self.config['results_path'], 'a') as file:
            for result in results:
                file.write(result)
                file.write('\n')
                self.query_sizes[result[:2]] += 1
                logging.info(f'page: {self.curr_results_page} | result: {result}')

    def get_results(self):
        t_sleep = MIN_TIME_SLEEP
        self.curr_results_page = 0

        for _ in range(1+N_RETRIES):
            try:
                logging.info('action: poll_results | result: in_progress')
                return self.poll_results()

            except socket.timeout:
                time.sleep(t_sleep)
                t_sleep = min(t_sleep*TIME_SLEEP_SCALE, MAX_TIME_SLEEP)

            # conn failed
            except SocketBroken:
                if not self.connect(self.config["results_ip"], self.config["results_port"], timeout=SOCK_TIMEOUT, tries=1+N_RETRIES):
                    logging.error(f'action: poll_results | result: fail | reason: connection refused {1+N_RETRIES} times')
                    return False

            # except Exception as e:
            #    if not self.signal_received:
            #        logging.error(f'action: polling | result: fail | error: {e}')
            #    return False
        return False

    def poll_results(self):
        logging.debug('action: polling | result: in_progress')
        keep_running = True
        t_sleep = MIN_TIME_SLEEP

        while keep_running:
            t, msg_id, value = self.protocolHandler.poll_results(self.curr_results_page)
            if self.protocolHandler.is_result_wait(t):
                logging.debug(f'action: polling | result: wait | page: {self.curr_results_page}')
                time.sleep(t_sleep)
                t_sleep = min(TIME_SLEEP_SCALE*t_sleep, MAX_TIME_SLEEP)

            elif self.protocolHandler.is_result_eof(t):
                logging.info('action: polling | result: eof | page: {self.curr_results_page}')
                keep_running = False

            elif self.protocolHandler.is_results(t):
                logging.debug(f'action: polling | result: success | len(results): {len(value)} | page: {self.curr_results_page}')
                t_sleep = max(t_sleep/TIME_SLEEP_SCALE, MIN_TIME_SLEEP)
                self.save_results(value)
                self.curr_results_page += 1     # increase page once result is saved
            else:
                logging.error(f'action: polling | result: fail | unknown_type: {t}')
        return True

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
