from utils.middleware.middleware import ACK


class TestMiddleware:
    def __init__(self):
        self.callbacks = {}
        self.messages = []
        self.sent = []
        self.sent_by_tag = {}
        self.callback_counter = 0

    def add_message(self, msg, q_name):
        self.messages.append((msg, q_name))

    def start(self):
        while len(self.messages) != 0:
            _messages = self.messages.copy()
            for msg, q_name in _messages:
                r = self.callbacks[q_name](msg, q_name)
                if r == ACK:
                    self.messages.remove((msg, q_name))
                else:
                    self.messages.remove((msg, q_name))
                    self.requeue_msg(msg, q_name)
                self.callback_counter += 1

    def stop(self):
        return

    def requeue_msg(self, msg, q_name):
        self.messages.append((msg, q_name))

    def requeue(self):
        if self.messages:
            self.messages.append(self.messages.pop(0))

    def consume(self, queue_name: str, callback):
        self.callbacks[queue_name] = callback

    def subscribe(self, topic: str, tags: list, callback, queue_name: str = None):
        self.callbacks[queue_name] = callback
        return 'test-queue'

    def produce(self, data, out_queue_name):
        if out_queue_name not in self.sent_by_tag:
            self.sent_by_tag[out_queue_name] = []
        self.sent_by_tag[out_queue_name].append(data)
        self.sent.append(data)

    def publish(self, data, topic, tag):
        if tag not in self.sent_by_tag:
            self.sent_by_tag[tag] = []
        self.sent_by_tag[tag].append(data)
        self.sent.append(data)
