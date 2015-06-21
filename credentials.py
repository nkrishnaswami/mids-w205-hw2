from ConfigParser import RawConfigParser

class Credentials(object):
    def __init__(self, filename):
        cp = RawConfigParser()
        with open(filename) as cfg:    
            cp.readfp(cfg)
            self.consumer_key = cp.get('consumer', 'key')
            self.consumer_secret = cp.get('consumer', 'secret')
            self.access_token = cp.get('access', 'token')
            self.access_token_secret = cp.get('access', 'secret')
