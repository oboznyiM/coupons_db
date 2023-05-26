import threading, time


class Subscriber(threading.Thread):
    def __init__(self, id, r):
        threading.Thread.__init__(self)
        self.redis = r
        self.pubsub = self.redis.pubsub()
        self.pubsub.subscribe(f"maksym-venues:{id}")
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        while not self.stop_event.is_set():
            message = self.pubsub.get_message()
            if message and message["type"] == "message":
                print(f'Received: {message["data"]}')
            time.sleep(0.001)
