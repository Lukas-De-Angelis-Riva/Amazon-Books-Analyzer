class TestMiddleware:
    def __init__(self):
        self.callback = None
        self.messages = []
        self.sent = []
        self.sent_by_tag = {}
        self.callback_counter = 0

    def add_message(self, msg):
        self.messages.append(msg)

    def start(self):
        _messages = self.messages.copy()
        for msg in _messages:
            self.callback(msg, 'test-key')
            self.messages.remove(msg)
            self.callback_counter += 1

    def stop(self):
        return

    def consume(self, queue_name: str, callback):
        self.callback = callback

    def subscribe(self, topic: str, tags: list, callback, queue_name: str = None):
        self.callback = callback
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
