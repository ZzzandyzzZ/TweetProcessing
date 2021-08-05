from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
import json

CONSUMER_KEY = ''
CONSUMER_SECRET = ''
ACCESS_TOKEN = ''
ACCESS_TOKEN_SECRET = ''
list_data = []
json_data = []
class StdOutListener(StreamListener):
    def on_data(self, Data):
        json_ = json.loads(Data)
        if hasattr(json_, 'retweeted_status') and hasattr(json_.retweeted_status, 'extended_tweet'):
            text=json_.retweeted_status.extended_tweet['full_text']
        if hasattr(json_, 'extended_tweet'):
            text= json_.extended_tweet['full_text']
        else:
            text= json_.get('text','NoneText')
        user_location = json_.get('user',{}).get('location','NoneCountryUser')
        if user_location is None:
            user_location='NoneLocation'
        tweet_location = json_.get('place','NoneCountryTweet')
        if tweet_location is None:
            tweet_location = 'NoneCountryTweet'
        else:
            tweet_location = json_.get('place',{}).get('country','NoneCountryTweet')
        selected_data=";;;".join((
            json_.get('created_at','NoneCreated'),
            json_.get('lang','NoneLang'),
            json_.get('user',{}).get('id_str','NoneUser'),
            user_location,
            tweet_location,
            text.replace("\n", " "))
        )
        data=selected_data
        list_data.append(data)
        json_data.append(json_)
        producer.send("tweet",data.encode('utf-8'))
        obj = open('data.txt', 'a')
        obj.write(data+"\n")
        if len(list_data)>=1000:
            obj.close
            with open('data.json', 'w') as outfile:
                json.dump(json_data, outfile,sort_keys = True, indent = 4, ensure_ascii = False)
            return False
        return True

    def on_error(self, status):
        print (status)
        if status == 420:
            print("API DESCONECTADA")
            return False

"""
track=[]
query_size=2
while query_size:
    query=input("Ingresa par de palabras clave")
    track.append(query)
    query_size-=1
"""
producer = KafkaProducer(bootstrap_servers='localhost:9092')
l = StdOutListener()
auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
stream = Stream(auth, l, tweet_mode='extended', timeout=10000)
stream.filter(track=['covid','peru'])

print("STREAM TERMINADO")