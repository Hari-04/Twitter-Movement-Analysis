# -*- coding: utf-8 -*-

import pickle
from elasticsearch import Elasticsearch
from datetime import datetime
from elasticsearch import helpers
import json
import re
import geocoder
from bs4 import BeautifulSoup
from urllib.request import urlopen
URL_INIT = 'https://twitter.com/'

def getUserAddress(user):
    #print (user)
    def parse_url(tweet_user):
        url = URL_INIT+ tweet_user.strip('@')
        return url

    try:
        url = parse_url(user)
        response = urlopen(url)
    except:
        return None

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
                #print (geocode)
                return geocode
            else:
                #return location
                #print "---No location--"
                return None
        except GeocoderTimedOut as e:
            print("Error: geocode failed on input %s with message %s"%(location, e))

es_buffer = []
count = 0
es = Elasticsearch(['0.0.0.0'])
e = pickle.load(open("total_tweets_harvey.pkl", "rb"))
print (len(e))
for msg in e:
    tweet = msg.__dict__
    tweet.pop('_id',None)
    tweet.pop('entity', None)
    tweet['geocode'] = getUserAddress(tweet['user'])
    tweet['entities'] = ['Harvey','Weinstein']
    tweet['_index'] = "es-test"
    tweet['_type'] = "metoo"
    print (tweet)
    break
    #tweet = json.dumps(tweet)
    #print (tweet)
    #break
    #tweet = json.loads(msg.value)

    if count == 10:
        helpers.bulk(es,es_buffer)
        count = 0
        es_buffer = []
        break
        #es.index(index='es-test',doc_type='metoo',body=tweet)
    else:
        es_buffer.append(tweet)
    count += 1

#es.indices.refresh(index="es-test")
helpers.bulk(es,es_buffer)
