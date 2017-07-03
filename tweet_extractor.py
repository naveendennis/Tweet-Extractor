import tweepy
import configparser
from tweepy.error import TweepError
import os.path
import csv


class TweetExtractor:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('config.ini')
        self.consumer_key = self.config['DEFAULT']['consumer_key']
        self.consumer_secret = self.config['DEFAULT']['consumer_secret']
        self.access_token = self.config['DEFAULT']['access_token']
        self.access_token_secret = self.config['DEFAULT']['access_token_secret']
        self.cache_filename = self.config['DEFAULT']['cache_filename']
        self.headers = self.config['DEFAULT']['headers'].split()
        auth = tweepy.OAuthHandler(self.consumer_key, self.consumer_secret)
        self.api = tweepy.API(auth)
        self.tweet_ids = self.load()
        self.output_filename = self.config['DEFAULT']['output_filename']
        self.error_filename = self.config['DEFAULT']['error_filename']
        self.__extract__()

    def load(self):
        with open(self.config['DEFAULT']['ids_filename'], 'r') as f:
            data = f.read()
        return data

    @staticmethod
    def __get_method__(obj, method_name='__getitem__', param=''):
        try:
            return getattr(obj, method_name)(param)
        except KeyError:
            return None

    def __update_csv_file__(self, data):
        has_header = False
        if os.path.exists(self.output_filename):
            has_header = True
        with open(self.output_filename, "a") as csv_file:
            writer = csv.writer(csv_file, delimiter=',')
            if not has_header:
                writer.writerow(self.headers)
            writer.writerow(data)
        with open(self.cache_filename, 'w') as f:
            f.write(str(data[0]))

    def __update_error_record__(self, tweet_id, error_message):
        is_exists = False
        if os.path.exists(self.error_filename):
            is_exists = True
        with open(self.error_filename, 'a') as f:
            writer = csv.writer(f, delimiter=',')
            if not is_exists:
                writer.writerow(['Tweet ID',
                                 'Error Message'])
            writer.writerow([tweet_id, error_message])

    def __extract__(self):
        last_id = None
        if os.path.exists(self.cache_filename):
            with open(self.cache_filename, 'r') as cache_file:
                last_id = cache_file.read()
        flag = False
        for each in self.tweet_ids.split():
            if flag or last_id is None:
                try:
                    tweet = self.api.get_status(each)
                except TweepError as e:
                    self.__update_error_record__(tweet_id=each,
                                                 error_message=e.reason)
                    tweet = None
                if tweet is not None:
                    each_tweet = tweet._json
                    each_row = []
                    for each_header in self.headers:
                        if '.' not in each_header:
                            each_row.append(self.__get_method__(each_tweet, param=each_header))
                        else:
                            _tweet_cache = each_tweet
                            for each_key in each_header.split('.'):
                                _tweet_cache = self.__get_method__(_tweet_cache, param=each_key)
                                if _tweet_cache is None:
                                    break
                                each_row.append(_tweet_cache)
                    self.__update_csv_file__(each_row)
            elif last_id == each:
                last_id = each
                flag = True


if __name__ == '__main__':
    TweetExtractor()

