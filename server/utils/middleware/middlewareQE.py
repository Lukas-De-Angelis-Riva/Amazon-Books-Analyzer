import logging
from utils.middleware.middleware import Middleware

class MiddlewareQE(Middleware):
    def __init__(self, in_queue_name: str, exchange: str, tag: str):
        super().__init__()

        # Declare IN-queue
        self.in_queue_name = in_queue_name

        # Declare results exchange
        self.tag = tag
        self.exchange = exchange

    def listen(self, callback):
        self.consuming_queue(callback, self.in_queue_name)

    def publish(self, results):
        self.send_msg(routing_key=self.tag, data=results, exchange=self.exchange)

    def resend(self, data):
        self.send_msg(routing_key=self.in_queue_name, data=data, exchange='')

    def change_tag(self, newTag):
        self.tag = newTag