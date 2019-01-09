from twitterscraper import query_tweets
import datetime as dt
from pymongo import MongoClient
import re
#import urllib2
from urllib.request import urlopen
from bs4 import BeautifulSoup
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from nltk.sentiment.vader import SentimentIntensityAnalyzer 
import pickle
import geocoder
URL_INIT = 'https://twitter.com/'

def getSentiment(sentence):
    analyser = SentimentIntensityAnalyzer()
    snt = analyser.polarity_scores(sentence)
    return snt["compound"]

def getUserAddress(user):
    def parse_url(tweet_user):
        url = URL_INIT+ tweet_user.strip('@')
        return url

    try:
        url = parse_url(user)
        response = urlopen(url)
    except:
        return None

    geolocator = Nominatim()
    html = response.read()
    soup = BeautifulSoup(html)
    location = soup.find('span','ProfileHeaderCard-locationText')
    if location:
        location = location.text.strip('\n').strip()
    if location:
        if ',' in location:
            splitted_location = location.split(',')
        else:
            splitted_location = re.split('|;|-|/|°|#', location)
        #print (splitted_location)
        try:
            if splitted_location:
                located_location = geocoder.yandex(splitted_location[0])
            else:
                located_location = geocoder.yandex(location)
            if located_location:
                geocode = str(located_location.latlng[0])+","+str(located_location.latlng[1])
                #print geocode
                return geocode
            else:
                #return location
                #print "---No location--"
                return None
        except GeocoderTimedOut as e:
            print("Error: geocode failed on input %s with message %s"%(location, e))
                
if __name__ == '__main__':
    #client = MongoClient("mongodb://usr:pwd@host/metoo")
    #db = client.metoo
    total_tweets = []
    for i in [2017,2018]:
        for j in range(1, 13):
            for k in range(1, 30):
                if j == 2 and k ==28:
                    break
                list_of_tweets = query_tweets("#MeToo weinstein", 10000, begindate=dt.date(i,j,k), enddate=dt.date(i,j,k+1))
                #list_of_tweets = query_tweets("metoo", 1, begindate=dt.date(2018,1,21), enddate=dt.date(2018,1,22))
                #list_of_tweets = [tweet.__dict__ for tweet in list_of_tweets]
                print(len(list_of_tweets))
                count = 0
                for tweet in list_of_tweets:
                    count += 1  
                    tweet = tweet.__dict__
                    tweet['sentiment'] = getSentiment(tweet['text'])
                    tweet['entity'] = ['Harvey','Weinstein']
                    
                    location = getUserAddress(tweet['user']); geocode = ""
                    if location:
                        #print (location)
                        geocode = location
                    tweet['geocode'] = geocode

                print(len(list_of_tweets))
                for tweet in list_of_tweets:
                    #print (tweet.__dict__)
                    #break
                    total_tweets.append(tweet)
                    #db.tweets.insert_one(tweet) 
                print(str(len(list_of_tweets)) + ' tweets written to database')
    print (len(total_tweets))
    with open('total_tweets_weinstein.pkl', 'wb') as f:
        pickle.dump(total_tweets, f)
             
   
    # list_of_tweets = query_tweets("#Trump", 100, begindate=dt.date(2016,1,1), enddate=dt.date(2016,2,2))
    # print(len(list_of_tweets))           

# db.createUser({
#     user: 'hari',
#     pwd: 'dcsc',
#     roles: [{ role: 'readWrite', db:'Test'}]
# })