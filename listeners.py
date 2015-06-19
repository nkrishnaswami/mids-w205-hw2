from __future__ import print_function
import tweepy

class EmittingListener(tweepy.StreamListener):
    """Listener that """
    def __init__(self, sink):
        self.sink = sink
        self.backoff_min = 1
    def on_connect(self):
        self.sink.open()
    def on_timeout(self):
        print("Timeout: reconnecting")
    def on_status(self, status):
        self.sink.emit(status)
    def on_error(self, code):
        print("Error: code={0}".format(code)
        self.sink.close()
        return False;
    def on_exception(self, code):
        print("Exception: code={0}".format(code)
        self.sink.close()
        return False;
