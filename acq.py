from __future__ import print_function
from contextlib import closing

import sys
import tweepy
import datetime
import urllib
import signal
import json
import os
import sys
import time
import re

from listeners import EmittingListener
from facets import FilteringFacet
from sinks import *

creds = Credentials(os.path.expanduser('~/.tweepy'))
auth = tweepy.OAuthHandler(creds.consumer_key, creds.consumer_secret)
auth.secure = True
auth.set_access_token(creds.access_token, creds.access_token_secret)

class RegexMatcher(object):
    def __init__(self, regex):
        self.re = re.compile(pattern, re.I)
    def check(self, record):
        matches = set(self.re.findall(record))
        if matches:
            return '_'.join(matches)
        return None
def make_sink(pattern):
    return RollingSink(tag + '/{0}', 100000)

tags = ['#NBAFinals2015', '#Warriors']
tags_re = '\b' + '|'.join(tags) + '\b'
with closing(FilteringFacet(RegexMatcher(tags_re), SinkType)) as f:
    l = EmittingListener(f)
    s = tweepy.Stream(auth, l)
    s.filter(f.filter, async=True)
print('All done!')
