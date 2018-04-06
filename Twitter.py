#Importing all the necessary libraries
import json
from twython import Twython
from pymongo import MongoClient
import time
# Enter your keys/secrets as strings in the following fields
credentials = {}
credentials['CONSUMER_KEY'] = "xxxxxxxxxxxxxxxxxxxxxxxxx"
credentials['CONSUMER_SECRET'] = "xxxxxxxxxxxxxxxxxxxxxxxxx"
credentials['ACCESS_TOKEN'] = "xxxxxxxxxxxxxxxxxxxxxxxxx"
credentials['ACCESS_SECRET'] = "xxxxxxxxxxxxxxxxxxxxxxxxx"

# Save the credentials object to file
with open("twitter_credentials.json", "w") as file:
    json.dump(credentials, file)


# Load credentials from json file
with open("twitter_credentials.json", "r") as file:
    creds = json.load(file)

# Instantiate an object
python_tweets = Twython(creds['CONSUMER_KEY'], creds['CONSUMER_SECRET'])

# Create our query
query = {'q': 'TDP',
        'result_type': 'recent',
        'count': 100,
        #'lang': 'en',
        'tweet_mode':'extended'
        }
client = MongoClient('localhost:27017')
db=client.twitter
tweets=db.Tweets
# Search tweets
dict_={}

# for status in python_tweets.search(**query)['statuses']:
#     dict_['ID'].append(status['id_str'])
#     dict_['user'].append(status['user']['screen_name'])
#     dict_['user_description'].append(status['user']['description'])
#     dict_['user_statuses_count'].append(status['user']['statuses_count'])
#     dict_['user_followers_count'].append(status['user']['followers_count'])
#     dict_['date'].append(status['created_at'])
#     dict_['text'].append(status['full_text'])
#     dict_['favorite_count'].append(status['favorite_count'])
#     dict_['retweet_count'].append(status['retweet_count'])
#     if not status['entities']['hashtags']:
#         dict_['hashtags'].append("")
#     else:
#         dict_['hashtags'].append(status['entities']['hashtags'][0]['text'])
#         # dict_['tagged_people'].append(status['entities']['user_mentions'][0]['name'])
#     # dict_['place'].append(status['place']['name'])
# # Structure data in a pandas DataFrame for easier manipulation
key=tweets.find({},{"_id":1})

tweet_id=[]
for i in key:
    tweet_id.append(i['_id'])

for status in python_tweets.search(**query)['statuses']:
    if 'RT @' not in status['full_text']:
        if status['id_str'] in tweet_id:
            favorite_count=tweets.find_one({"_id":status['id_str']},{"favorite_count":1,"_id":0})
            favorite_count['favorite_count'].append(status['favorite_count'])
            tweets.update_one({"_id":status['id_str']},{'$set':{"favorite_count":favorite_count}})
            #dict_[status['id_str']]['favorite_count'].append(status['favorite_count'])
            #dict_[status['id_str']]['retweet_count'].append(status['retweet_count'])
            retweet_count = tweets.find_one({"_id": status['id_str']}, {"retweet_count": 1, "_id": 0})
            retweet_count['retweet_count'].append(status['retweet_count'])
            tweets.update_one({"_id": status['id_str']}, {'$set': {"retweet_count": retweet_count}})

        else:
            dict_[status['id_str']]={'_id':status['id_str'],'user': status['user']['screen_name'],'user_description':status['user']['description'],'user_statuses_count':status['user']['statuses_count'],
                             'user_followers_count':status['user']['followers_count'],'date': status['created_at'], 'text': status['full_text'],
                             'favorite_count': [status['favorite_count']],'retweet_count':[status['retweet_count']],'hashtags':[],'tagged_people':[],'place':[],'Sentiment':""}
            if not status['entities']['hashtags']:
                 dict_[status['id_str']]['hashtags'].append("")
            else:
                 dict_[status['id_str']]['hashtags'].append(status['entities']['hashtags'][0]['text'])

tweets.insert(dict_.values())
time.sleep(15*60)
