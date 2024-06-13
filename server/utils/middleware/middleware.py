import pika     # type: ignore
import logging
import traceback
logging.getLogger('pika').setLevel(logging.ERROR)

HOST = 'rabbitmq'
STOP = 0
ACK = 1
NACK = 2


class ChannelAlreadyConsuming(Exception):
    pass


class Middleware:
    def __init__(self):
        self.connection = pika.BlockingConnection(
                               pika.ConnectionParameters(host=HOST))
        self.channel = self.connection.channel()
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

    def stop(self):
        self.channel.stop_consuming()

    def __make_callback(self, callback):
        def __wrapper(ch, method, properties, body):
            response = callback(body, method.routing_key)
            if response == STOP:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                self.stop()
                return
            elif response == ACK:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            elif response == NACK:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            else:
                logging.error(f"action: callback | unexpected value: {response}")
                raise RuntimeError(f"Unexpected value: {response}")

        return __wrapper

    def consume(self, queue_name: str, callback):
        logging.debug(f"action: consume | qname: {queue_name}")
        self.channel.basic_consume(queue=queue_name,
                                   on_message_callback=self.__make_callback(callback))

    def subscribe(self, topic: str, tags: list, callback, queue_name: str = None):
        if len(tags) == 0:
            tags = ['']

        if not queue_name:
            logging.debug("action: subscribe | setting_up | qname: not specified")
            result = self.channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue
            logging.debug(f"action: subscribe | creating_queue | qname: {queue_name}")
        else:
            logging.debug(f"action: subscribe | setting_up | qname: {queue_name}")

        for tag in tags:
            self.channel.queue_bind(exchange=topic, queue=queue_name, routing_key=tag)
            logging.debug(f"action: subscribe | binding_queue | qname: {queue_name} | topic/tag: {topic}/{tag}")

        self.consume(queue_name, callback)
        return queue_name

    def __send_msg(self, data, exchange: str, routing_key: str):
        self.channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=data,
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            )
        )

    def produce(self, data, out_queue_name):
        return self.__send_msg(data=data, exchange='', routing_key=out_queue_name)

    def requeue(self, data, in_queue_name):
        # Same as produce, but better semantic
        return self.produce(data, in_queue_name)

    def publish(self, data, topic, tag):
        return self.__send_msg(data=data, exchange=topic, routing_key=tag)
