import logging
import signal

from multiprocessing import Process
from utils.messageHandler import MessageHandler, MessageType


class HeartBeat(Process):
    def __init__(self, addr):
        super().__init__(name='HeartBeat', args=())
        self.UDPHandler = MessageHandler(addr[0], addr[1])
        self.on = True

    def run(self):
        signal.signal(signal.SIGTERM, self.__handle_signal)
        while self.on:
            message, addr = self.UDPHandler.receive_message()
            if addr:
                self.UDPHandler.send_message(addr=addr, type=MessageType.HEARTBEAT, id=message["id"])

        self.UDPHandler.close()
        logging.info('action: heartbeat | result: finish')

    def __handle_signal(self, signum, frame):
        logging.info('action: stop_heartbeat | result: in_progress')
        self.on = False
        self.UDPHandler.close()
        logging.info('action: stop_heartbeat | result: success')
