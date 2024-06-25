import logging
import signal

from multiprocessing import Process
from common.messageHandler import MessageHandler

class HeartBeat(Process):
    def __init__(self, UDPHandler):
        super().__init__(name='HeartBeat', args=())
        self.UDPHandler = UDPHandler
        self.on = True

    def run(self):
        signal.signal(signal.SIGTERM, self.__handle_signal)
        while self.on:
            message, addr = self.UDPHandler.receive_message()
            if addr:
                self.UDPHandler.send_message(message)

        self.UDPHandler.close()
        logging.info('action: heartbeat | result: finish')

    def __handle_signal(self, signum, frame):
        logging.info('action: stop_heartbeat | result: in_progress')
        self.on = False
        self.UDPHandler.close()
        logging.info('action: stop_heartbeat | result: success')