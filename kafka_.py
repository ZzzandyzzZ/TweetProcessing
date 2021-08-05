import tweepy
import json
import time
from datetime import datetime
import pandas as pd
from kafka import KafkaProducer

CONSUMER_KEY = 'EotPmGTtvHl2OOeyYzuRb1cXt'
CONSUMER_SECRET = 'kgSFyfaFBoH3otR0BkCnoev1QjHSyfZd9qnq3d3daVEijwTyKO'
ACCESS_TOKEN = '2707859331-IcdDb5HqbNDMWQdw4u3ogYeenG3kCEAkw2jXEXP'
ACCESS_TOKEN_SECRET = 'qLjerPNOCF5LpCi6ZIKAQUFVx0Jf9uESPm2iiOW1qjWVJ'

def accept_tweet(tweet):
    THRESHOLD = 3
    interactions = tweet['retweet_count'] + tweet['favorite_count']
    if interactions >= THRESHOLD and 'RT ' not in tweet['full_text']:
        return True
    return False


auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

api = tweepy.API(auth,wait_on_rate_limit=True,wait_on_rate_limit_notify=True)
min_popularity = 0
max_popularity = 0
tweets = []
for tweet_parsed in tweepy.Cursor(api.search, q="#Basketball",tweet_mode="extended").items(100):
    tweet_str= json.dumps(tweet_parsed._json,indent=2)
    tweet = tweet_parsed._json
    if accept_tweet(tweet):
        tweet_data = {
            'user_id': tweet['user']['id_str'],
            'user_name': tweet['user']['name'],
            'user_location': tweet['user']['location'],
            'tweet_msg': tweet['full_text']
        }
        tweets.append(tweet_data)



tweet_str = json.dumps(tweets, indent=2)

print(tweet_str)

# archivo="datos_twiter.csv"
# csv=open(archivo,"w")
# titles="user_id,user_name,popularity,user_location,tweet_msg"

def normalize_timestamp(time):
    mytime=datetime.strptime(time, '%Y-%m-%d %H:%M:%S')
    return (mytime.strftime('%Y-%m-%d %H:%M:%S'))

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
topic_name='data'
def get_twitter_data():
    for i in tweepy.Cursor(api.search, q="#Basketball",tweet_mode="extended").items(10):
        tweet_str= json.dumps(tweet_parsed._json,indent=2)
        tweet = tweet_parsed._json
        record='Start:'+ tweet['user']['id_str'] +';'+ tweet['user']['name']+';'+tweet['user']['location']+';'+ tweet['full_text']
        producer.send(topic_name,record)
get_twitter_data()

def periodic_work(interval):
    while True:
        get_twitter_data()
        time.sleep(interval)

periodic_work(60*0.1)