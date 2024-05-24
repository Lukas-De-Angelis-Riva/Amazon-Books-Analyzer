import logging
from utils.middleware.middleware import Middleware

class MiddlewareQQ(Middleware):
    def __init__(self, in_queue_name: str, out_queue_name: str):
        super().__init__()

        # Declare IN-queue
        self.in_queue_name = in_queue_name

        # Declare OUT-queue
        self.out_queue_name = out_queue_name

    def listen(self, callback):
        self.consuming_queue(callback, self.in_queue_name)
    
    def publish(self, data):
        self.send_msg(routing_key=self.out_queue_name, data=data, exchange='')

    def resend(self, data):
        self.send_msg(routing_key=self.in_queue_name, data=data, exchange='')