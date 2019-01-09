# -*- coding: utf-8 -*-

from pykafka import KafkaClient
import json


client = KafkaClient("0.0.0.0:9092")
topic = client.topics['dumpTweets']
consumer = topic.get_simple_consumer()
#it = consumer.__iter__()

'''
for msg in consumer:
    data = json.loads(msg.value)
    print data
'''
print consumer.consume().value
print consumer.consume().value