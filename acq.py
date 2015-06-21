from __future__ import print_function

import tweepy
import datetime
import os

from credentials import Credentials
from collector import Collector


def main():
    creds = Credentials(os.path.expanduser('~/.tweepy'))
    today = datetime.date.today()
    week = datetime.timedelta(7)
    query_terms=['#NBAFinals2015', '#Warriors']
    stream = False;
    if stream:
        auth = tweepy.OAuthHandler(creds.consumer_key, creds.consumer_secret)
        auth.set_access_token(creds.access_token, creds.access_token_secret)
        query_ops={'until': today+week}
    else:
        #Per Ron Cordell's suggestion, use App Auth token for increased limits
        auth = tweepy.AppAuthHandler(creds.consumer_key, creds.consumer_secret)
        query_ops={'since': today-week,
                   'until': today}
    collector = Collector(auth)
    if stream:
        print("Starting streaming")
        collector.stream(query_terms=query_terms,
                  query_ops=query_ops)
    else:
        print("Starting search")
        collector.search(query_terms=query_terms,
                  query_ops=query_ops)
    print('All done!  Last ID processed={0}'.format(collector.last_id))

if __name__ == '__main__':
    main()
