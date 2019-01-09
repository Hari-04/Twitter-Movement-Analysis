# -*- coding: utf-8 -*-

import os
import re
from streamparse import Bolt
import json
from nltk.tag import StanfordNERTagger
from nltk.tokenize import word_tokenize 
import spacy
import nltk
from elasticsearch import Elasticsearch
from datetime import datetime
from elasticsearch import helpers
from kafka import KafkaProducer

class EntityBolt(Bolt):
    outputs = ['tweet']

    def initialize(self, conf, ctx): 
        self.pid = os.getpid()
        nltk.download('vader_lexicon')
        local_ip = "localhost:9092"
        server_ip = "34.236.192.205:9092"
        self.producer = KafkaProducer(bootstrap_servers=[server_ip])
        self.topic = "esTweets"
        #self.es = Elasticsearch(['52.11.2.94'])

    def getEntities(self, sentence):
        
        st = StanfordNERTagger('/home/ubuntu/english.all.3class.distsim.crf.ser.gz',
    					   '/home/ubuntu/stanford-ner.jar',
    					   encoding='utf-8')
        tokenized_text = word_tokenize(sentence)
        classified_text = [i[0] for i in st.tag(tokenized_text) if i[1] != "O"]
        stanfordNames = [i[0] for i in st.tag(tokenized_text) if i[1] == "PERSON"]
        #text = "#BUSINESSofTheDay : #Bankruptcy Reveals Wild List: Who Weinstein Owes #Money To: Malia Obama, #DavidBowie, #MichaelBay http://a.msn.com/0C/en-us/BBKu067?ocid=st … #JudiDench #QuentinTarantinao #MichaelBay #DanielRadcliffe #RobertDeNiro #SexualAssault #MeToo #TimesUp #USA #EU #UK"
        #print sentence
        
        nlp = spacy.load('en')
        text = nlp(sentence.decode("utf-8"))
        #return [X.text for X in text.ents if X.label_ == "PERSON"]
        
        res = set([])
        for X in text.ents:
            if X.label_ == "PERSON":
                name = X.text.split()
                if len(name) > 1:
                    if name[0] in classified_text and name[1] in classified_text:
                        res.add(X.text)
                    elif name[0] in classified_text:
                        res.add(name[0])
                    elif name[1] in classified_text:
                        res.add(name[1])
                else:
                    res.add(X.text)
                    
        for name in stanfordNames:
            res.add(name)
        for word in sentence.split():
            if word[0] == "@":
                res.add(word[1:])
        
        return res
        
    def process(self, tup):
        tweet = tup.values[0]
        #tweet['text'] = re.sub(r'[^\x00-\x7F]+','',tweet['text'])
        #self.logger.info("\n [INFO_BOLT_TWEET] : "+ str(tweet['text']))
        
        entities = ",".join(self.getEntities(tweet['text']))
        tweet['entities'] = entities
        tweet['movement'] = "metoo"
        self.logger.info("\n [INFO_ENTI_BOLT_TWEET] : "+ str(tweet['text'])+"---"+str(tweet['entities']))
        #self.es.index(index='tweets',doc_type='metoo',body=tweet)
        self.producer.send(self.topic, json.dumps(tweet))
        self.emit([tweet])
