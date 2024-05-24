import logging
from utils.middleware.middleware import Middleware

class MiddlewareEQ(Middleware):
    def __init__(self, exchange: str, tag: str, out_queue_name: str):
        super().__init__()
        self.in_queue = None
        self.out_queue_name = None

        if exchange is not None:
            # Declare IN exchange (publisher-subscriber)
            result = self.channel.queue_declare(queue='', exclusive=True)
            self.in_queue = result.method.queue
            self.channel.queue_bind(exchange=exchange, queue=self.in_queue, routing_key=tag)

        if out_queue_name is not None:
            # Declare OUT queue (producer-consumer)
            self.out_queue_name = out_queue_name

    def listen(self, callback):
        if self.in_queue is not None:
            self.consuming_queue(callback, self.in_queue)

    def publish(self, data):
        if self.out_queue_name is not None:
            self.send_msg(routing_key=self.out_queue_name, data=data, exchange='')

    def resend(self, data):
        return