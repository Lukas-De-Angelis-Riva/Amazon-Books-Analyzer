import socket
from common.handler import TCPHandler
import logging
import re

class TestClient:
    def __init__(self, ip, port):
        # Initialize server socket
        self.ip = ip
        self.port = port
        self.socket = None
        self.handler = None

    def run(self):
        logging.info(f'action: running test-client')
        # Read airports.csv and send to the system.
        self.connect(self.ip, self.port)
        self.say_hello()
        self.listen_hello()
        self.disconnect()
        logging.info(f'action: closing test-client')

    def connect(self, ip, port):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((ip, port))
        self.handler = TCPHandler(self.socket)

    def disconnect(self):
        self.socket.close()

    def say_hello(self):
        logging.info(f'action: saying hello | result: in progress')
        self.handler.send_all(str.encode("hello"))
        logging.info(f'action: saying hello | result: done')
        return

    def listen_hello(self):
        logging.info(f'action: listening hello | result: in progress')
        return self.handler.read(5)
        logging.info(f'action: listening hello | result: done')
