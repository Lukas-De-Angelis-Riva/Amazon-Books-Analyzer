import logging
import signal

from multiprocessing import Process
from common.messageHandler import MessageHandler

class HeartBeat(Process):
    def __init__(self, ip, port):
        super().__init__(name='HeartBeat', args=())
        self.UDPHandler = MessageHandler(ip, port)
        self.on = True

    def run(self):
        signal.signal(signal.SIGTERM, self.__handle_signal)
        while self.on:
            message, addr = self.UDPHandler.receive_message()
            if addr:
                self.UDPHandler.send_message(message)

        self.UDPHandler.close()

    def __handle_signal(self, signum, frame):
        logging.debug('action: stop_heartbeat | result: in_progress')
        self.on = False
        self.UDPHandler.close()
        logging.debug('action: stop_heartbeat | result: success')