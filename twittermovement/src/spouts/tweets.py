from itertools import cycle

from streamparse import Spout
from pykafka import KafkaClient
import json

class KafkaSpout(Spout):
    outputs = ['tweet']

    def initialize(self, stormconf, context):
        server_ip = "34.236.192.205:9092"
        local_ip = "localhost:9092"
        client = KafkaClient(local_ip)
        self.topic = client.topics['dumpTweets']
        self.consumer = self.topic.get_simple_consumer()
        #self.it = self.consumer.__iter__()
        self.count = 0
        
    def next_tuple(self):
        try:
            self.count += 1
            tweet = self.consumer.consume(block=False)
            #with open("tweet.txt", "w") as f:
            #    f.write(tweet.value+"\n")
            self.logger.info(tweet.value)
            msg = json.loads(tweet.value)
            if self.count % 5000:
                self.logger.info("-------TWEETS PROCESSED:"+str(self.count)+"--------")
            self.emit([msg])
        except:
            #self.logger.info("ERROR IN SPOUT next_tuple")
            pass