# -*- coding: utf-8 -*-

import os
from collections import Counter
import re
from streamparse import Bolt
import json
from nltk.sentiment.vader import SentimentIntensityAnalyzer 

class SentimentBolt(Bolt):
    outputs = ['tweet']

    def initialize(self, conf, ctx):
        self.counter = Counter()
        self.pid = os.getpid()
        self.total = 0

    def getSentiment(self, sentence):
        analyser = SentimentIntensityAnalyzer()
        snt = analyser.polarity_scores(sentence)
        return snt["compound"]

    def process(self, tup):
        tweet = tup.values[0]
        #tweet['text'] = re.sub(r'[^\x00-\x7F]+','',tweet['text'])
        #self.logger.info("\n [INFO_BOLT_TWEET] : "+ str(tweet['text']))
        
        sentiment = self.getSentiment(tweet['text'])
        tweet['sentiment'] = sentiment
        self.logger.info("\n [INFO_SENTI_BOLT_TWEET] : "+ str(tweet['text'])+" "+str(tweet['sentiment']))
        self.emit([tweet])
