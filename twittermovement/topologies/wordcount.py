"""
Word count topology
"""

from streamparse import Grouping, Topology

from bolts.process import ProcessBolt
from bolts.sentiment import SentimentBolt
from bolts.entity import EntityBolt
from spouts.tweets import KafkaSpout


class WordCount(Topology):
    word_spout = KafkaSpout.spec()
    process_bolt = ProcessBolt.spec(inputs=[word_spout],par=2)
    senti_bolt = SentimentBolt.spec(inputs={process_bolt: Grouping.fields('tweet')}, par=2)
    entity_bolt = EntityBolt.spec(inputs={senti_bolt: Grouping.fields('tweet')}, par=2)
