class TestMiddleware2:
    def __init__(self):
        self.callback = None
        self.messages = []

    def add_message(self, msg, q_name):
        self.messages.append((msg, q_name))

    def start(self):
        while len(self.messages) != 0:
            _messages = self.messages.copy()
            for msg, q_name in _messages:
                self.callback(msg, q_name)
                self.messages.remove((msg, q_name))

    def stop(self):
        return

    def consume(self, queue_name: str, callback):
        self.callback = callback

    def subscribe(self, topic: str, tags: list, callback, queue_name: str = None):
        self.callback = callback
        return 'test-queue'

    def produce(self, data, out_queue_name):
        return

    def publish(self, data, topic, tag):
        return
