# -*- coding: utf-8 -*-
import re
from kafka import KafkaConsumer


def processTweets(sentence):
    sentence = sentence.split(" "); formatted_sentence = list()
    pattern = re.compile("[A-Z][a-z]+")
    for word in sentence:
        if "#" in word and len(word)>2:
            word = word[1].upper()+word[2:]            
            for m in pattern.finditer(word):
                formatted_sentence.append(m.group())
        else:
            formatted_sentence.append(word)
    return re.sub(r'[^\x00-\x7F]+',''," ".join(formatted_sentence).strip())


def getTweets():
    
    res = []
    consumer = KafkaConsumer("tweet", bootstrap_servers="0.0.0.0:9092", auto_offset_reset='earliest')
    try:
        for message in consumer:
            res.append(list(map(processTweets,message.text.split("."))))  
    except KeyboardInterrupt:
        print "Error in reading from kafka"
    return res
    
