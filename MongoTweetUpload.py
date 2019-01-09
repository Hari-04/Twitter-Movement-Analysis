# -*- coding: utf-8 -*-

from pymongo import MongoClient
from twitterscraper import query_tweets
import datetime as dt 

#client = MongoClient('localhost', 27017)
#db = client['Test']
#collection = db['Sample']
list_of_tweets = query_tweets("#MeToo", 1, begindate=dt.date(2018,1,21), enddate=dt.date(2018,1,22))
#for tweet in list_of_tweets[0]:
#    collection.insert_one(tweet.__dict__)

