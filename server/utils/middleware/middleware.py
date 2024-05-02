import pika
import logging
import traceback
logging.getLogger('pika').setLevel(logging.ERROR)

HOST = 'rabbitmq'

class ChannelAlreadyConsuming(Exception):
    pass

class Middleware:
    def __init__(self):
        self.connection = pika.BlockingConnection(
                               pika.ConnectionParameters(host=HOST))
        self.channel = self.connection.channel()
        self.callback_func = self.__no_callback
        self.active_channel = False

    def start(self):
        try:
            self.channel.start_consuming()
        except Exception as e:
            logging.error(f'action: pika_consume | result: fail | error: {str(e)}')
            logging.error(traceback.format_exc())
        finally:
            self.channel.close()
            self.connection.close()

    def callback(self, ch, method, properties, body):
        keep_going = self.callback_func(body)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        if not keep_going:
            self.stop()
            return

    def stop(self):
        self.channel.stop_consuming()

    def send_msg(self, routing_key, data, exchange=''):
        self.channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=data,
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
        ))

    def consuming_queue(self, callback, queue_consume):
        if self.active_channel:
            raise ChannelAlreadyConsuming()

        self.active_channel = True
        self.callback_func = callback
        logging.debug(f"action: consuming_queue | queue: {queue_consume}")
        self.channel.basic_consume(queue=queue_consume,
                                   on_message_callback=self.callback)

    def __no_callback(self, body):
        logging.error("No callback set")
