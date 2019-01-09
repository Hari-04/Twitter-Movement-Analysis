# -*- coding: utf-8 -*-

#from elasticsearch import Elasticsearch
from datetime import datetime
#from elasticsearch import helpers
#from  pykafka import KafkaClient
import json
import pickle

count = 0
es_buffer = []
#client = KafkaClient("0.0.0.0:9092")
#topic = client.topics['es-test']
#consumer = topic.get_simple_consumer()

#es = Elasticsearch(['34.237.76.193'])
#it = consumer.__iter__()

'''
for msg in consumer:
    data = json.loads(msg.value)
    print data
'''
#with open("total_tweets.pkl", "rb") as input_file:
e = pickle.load( open( "total_tweets_harvey.pkl", "rb" ) )
print (len(e))
#f = open("total_tweets_trump.pkl","wb")
#pickle.dump(e, f, protocol=2)

for msg in e:
    #print consumer.consume().value
    #tweet = json.loads(msg.value)
    #tweet.pop('_id',None)
    #tweet['_index'] = "es-test"
    #tweet['_type'] = "metoo"
    msg.timestamp = str(msg.timestamp)
    #print (msg.geocode)
    #msg.geocode = getUserAddress(msg.user)
    tweet = msg.__dict__
    tweet.pop('_id',None)
    tweet.pop('entity', None)
    tweet['geocode'] = getUserAddress(tweet['user'])
    tweet['entities'] = ['Harvey','Weinstein']
    tweet['_index'] = "es-test"
    tweet['_type'] = "metoo"
    break
    '''
    if count == 10:
        helpers.bulk(es,es_buffer)
        count = 0
        es_buffer = []
        #es.index(index='es-test',doc_type='metoo',body=tweet)
    else:
        es_buffer.append(tweet)
    count += 1
    '''
#es.indices.refresh(index="es-test")
#helpers.bulk(es,es_buffer)
