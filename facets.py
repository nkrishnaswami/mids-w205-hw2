from __future__ import print_function

class FilteringFacet(object):
    def __init__(self, matcher):
        self.sinks = {}
        self.matcher = matcher
    def emit(self, record):
        key = self.matcher.check(record)
        if key:
            sink = self.sinks[key]
            if not sink:
                sink = self.sinks[key] = self.matcher.make_sink(key)
            sink.emit(record)
            return True
        return False
    def close(self):
        for (key,sink) in self.sinks.iterpairs():
            print("Closing sink for key {0}".format(key));
            sink.close()
        self.sinks = {}
