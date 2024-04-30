import logging
from utils.middleware.middleware import Middleware

N_QUERIES = 5

class ResultHandlerMiddleware(Middleware):
    def __init__(self):
        super().__init__()

        # Declare results exchange (publisher-subscriber)
        self.channel.exchange_declare(exchange='results', exchange_type='direct')
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.result_queue_name = result.method.queue
        logging.debug(f'action: declare_in_exchange | exchange: results')

        for i in range(1, N_QUERIES+1):
            self.channel.queue_bind(exchange='results', queue=self.result_queue_name, routing_key=f'Q{i}')
            logging.debug(f'action: bind_queue | queue: {self.result_queue_name} | exchange: results | tag: Q{i}')
        self._callback_results = None

    def start(self):
        self.channel.start_consuming()

    def callback_results(self, ch, method, properties, body):
        self._callback_results(body, method.routing_key)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def listen_results(self, callback):
        self._callback_results = callback
        self.channel.basic_consume(queue=self.result_queue_name, on_message_callback=self.callback_results)