# ===============================================
# twitter-to-mongo.py v1.0 Created by Sam Delgado
# ===============================================
from pymongo import MongoClient
import json
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import datetime

# The MongoDB connection info. This assumes your database name is TwitterStream, and your collection name is tweets.
db = MongoClient('localhost', 27017).TwitterStream.tweets
db.tweets.ensure_index("id", unique=True, dropDups=True)
collection = db.tweets

# Add the keywords you want to track. They can be cashtags, hashtags, or words.
keywords = ['#Tmobile', '#Sprint']

# Optional - Only grab tweets of specific language
language = ['en']

# You need to replace these with your own values that you get after creating an app on Twitter's developer portal.
consumer_key = "zppuSEuJvADzNZkuyLXFYHcfI"
consumer_secret = "MbKLdXcyO6VxlSI48RevRllbEcDL4q5pKmd7bAqTtIzaEoAFNT"
access_token = "2225305578-utWNR1Fgr1nHqNJXYsU8UKFhpyAdya3Tg4chLOZ"
access_token_secret = "cPXG4OrkUPQQb1sQTDTF0dMbSH797UJ0ZGKfZFOphNSRQ"

# The below code will get Tweets from the stream and store only the important fields to your database
class StdOutListener(StreamListener):

    def on_data(self, data):

        # Load the Tweet into the variable "t"
        t = json.loads(data)

        # Pull important data from the tweet to store in the database.
        tweet_id = t['id_str']  # The Tweet ID from Twitter in string format
        username = t['user']['screen_name']  # The username of the Tweet author
        followers = t['user']['followers_count']  # The number of followers the Tweet author has
        text = t['text']  # The entire body of the Tweet
        hashtags = t['entities']['hashtags']  # Any hashtags used in the Tweet
        dt = t['created_at']  # The timestamp of when the Tweet was created

        # Convert the timestamp string given by Twitter to a date object called "created". This is more easily manipulated in MongoDB.
        created = datetime.datetime.strptime(dt, '%a %b %d %H:%M:%S +0000 %Y')

        # Load all of the extracted Tweet data into the variable "tweet" that will be stored into the database
        tweet = {'id':tweet_id, 'username':username, 'followers':followers, 'text':text, 'hashtags':hashtags, 'language':language, 'created':created}

        # Save the refined Tweet data to MongoDB
        collection.save(tweet)
        
        # Optional - Print the username and text of each Tweet to your console in realtime as they are pulled from the stream
        print (username,':',' ',text)
        return True

    # error code print with failsafe
    def on_error(self, status):
        if status == 420:
            # Returning False on_data method in case rate limit occurs.
            return False
        print(status)

# Some Tweepy code that can be left alone. It pulls from variables at the top of the script
if __name__ == '__main__':
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, l)
    stream.filter(track=keywords, languages=language)
