# -*- coding: utf-8 -*-

import os
from collections import Counter
import re
from streamparse import Bolt
import urllib2
from bs4 import BeautifulSoup
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
URL_INIT = 'https://twitter.com/'

class ProcessBolt(Bolt):
    outputs = ['tweet']

    def initialize(self, conf, ctx):
        self.counter = Counter()
        self.pid = os.getpid()
        self.total = 0
    
    def getUserAddress(self, user):
    
        def parse_url(tweet_user):
            url = URL_INIT+ tweet_user.strip('@')
            return url
        
        try:
            url = parse_url(user)
            response = urllib2.urlopen(url)
        except:
            return None
        
        geolocator = Nominatim()
        html = response.read()
        soup = BeautifulSoup(html)
        location = soup.find('span','ProfileHeaderCard-locationText').text.encode('utf8').strip('\n').strip()
        if location:
            if ',' in location:
                splitted_location = location.split(',')
            else:
                splitted_location = re.split('|;|-|/|°|#', location)
            try:
                if splitted_location:
                    located_location = geolocator.geocode(splitted_location[0], timeout=100)
                else:
                    located_location = geolocator.geocode(location, timeout=100)
                if located_location:
                    print located_location.latitude,located_location.longitude
                    return located_location
                else:
                    #return location
                    return None
            except GeocoderTimedOut as e:
                print("Error: geocode failed on input %s with message %s"%(location, e))
    
    def processTweets(self,sentence):
        sentence = sentence.split(" "); formatted_sentence = list()
        pattern = re.compile("[A-Z][a-z]+")
        for word in sentence:
            if "#" in word and len(word)>2 and "http" not in word:
                word = word[1].upper()+word[2:]            
                for m in pattern.finditer(word):
                    formatted_sentence.append(m.group())
            else:
                formatted_sentence.append(word)
        return re.sub(r'[^\x00-\x7F]+',''," ".join(formatted_sentence).strip())

    def process(self, tup):
        tweet = tup.values[0]
        if type(tweet) == dict:
            tweet['text'] = re.sub(r'[^\x00-\x7F]+','',tweet['text'])
            #self.logger.info("\n [INFO_BOLT_TWEET] : "+ str(tweet['text']))
            location = self.getUserAddress(tweet['user']); geocode = ""
            if location:
                geocode = str(location.latitude)+","+str(location.longitude)
            
            for sentence in tweet['text'].split("."):
                sentence = self.processTweets(sentence)
                tweet['text'] = sentence
                tweet['geocode'] = geocode
                self.logger.info("\n [INFO_BOLT_TWEET] : "+ str(tweet['text'])+" "+str(tweet['geocode']))
                self.emit([tweet])
