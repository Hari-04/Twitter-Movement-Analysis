# -*- coding: utf-8 -*-

from kafka import KafkaProducer
from kafka.errors import KafkaError
from pymongo import MongoClient


#kafka settings
producer = KafkaProducer(bootstrap_servers=['0.0.0.0:9092'])
topic = "tweets"

#mongo settings
client = MongoClient('localhost', 27017)
db = client['MeToo']
collection = db['Tweets']


for entry in collection.find({}):
    producer.send(topic, entry['text'].strip().encode('utf-8'))
    