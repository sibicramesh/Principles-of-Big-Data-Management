
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream


access_token = "1048610250-QQZ8D05FWBIon130QSgjg0XGDN0dw3lXXhP7KFt"
access_token_secret = "RRiMG6c7mIY61apEJWSwoxMMaSVN8tQwIcuK627ugp46r"
consumer_key = "RRAnQIWfiuDBpJm94OWgwmpEF"
consumer_secret = "uXj3hPKmkU931K8ye5FMZemBUky4UyEQxQCz2Ej5qyS4zp0Ddw"

class StdOutListener(StreamListener):

    def on_data(self, data):
        print(data)
        with open('fetched_tweets.json','a') as tf:
            tf.write(data)
        return True
    
if __name__ == '__main__':

    
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, StdOutListener())

    stream.filter(track=['olympics','cricket','fifa'])

