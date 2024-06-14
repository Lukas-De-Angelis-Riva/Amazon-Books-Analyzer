class TestMiddleware:
    def __init__(self):
        self.callback = None
        self.messages = []
        self.sent = []
        self.callback_counter = 0

    def add_message(self, msg):
        self.messages.append(msg)

    def start(self):
        for msg in self.messages:
            self.callback(msg, 'test-key')
            self.callback_counter += 1

    def stop(self):
        return

    def consume(self, queue_name: str, callback):
        self.callback = callback

    def subscribe(self, topic: str, tags: list, callback, queue_name: str = None):
        self.callback = callback
        return 'test-queue'

    def produce(self, data, out_queue_name):
        self.sent.append(data)

    def publish(self, data, topic, tag):
        self.sent.append(data)
