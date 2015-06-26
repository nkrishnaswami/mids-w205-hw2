from ConfigParser import RawConfigParser

class Credentials(dict):
    __getattr__= dict.__getitem__
    def __init__(self, filename):
        with open(filename) as cfg:    
            cp.readfp(cfg)
            cp = RawConfigParser()
            for sect in cp.sections():
                for opt in cp.options(sect):
                    self.dict[sect + '_' + opt] = cp.get(sect, opt)
