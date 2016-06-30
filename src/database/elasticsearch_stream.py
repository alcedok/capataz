"""
==========================================
Elasticsearch as Kafka Consumer from Topic 
==========================================

"""
# print(__doc__)
from kafka import KafkaConsumer
import json
from elasticsearch import Elasticsearch
import sys
import yaml
import signal

def yaml_loader(yaml_file):
	with open(yaml_file) as yml:
		config = yaml.load(yml)
	return config

def json_loader(json_file):
	with open(json_file) as jsn:
		config = json.load(jsn,object_hook=_tostring)
	return config

def signal_handler(signal,frame):
	print "--"
	print "--- elasticsearch consumer has been halted ---"
	print "--"
	sys.exit(0)

def _tostring(data, ignore_dicts = False):
    # if this is a unicode string, return its string representation
    if isinstance(data, unicode):
        return data.encode('utf-8')
    # if this is a list of values, return list of byteified values
    if isinstance(data, list):
        return [ _tostring(item, ignore_dicts=True) for item in data ]
    # if this is a dictionary, return dictionary of byteified keys and values
    # but only if we haven't already byteified it
    if isinstance(data, dict) and not ignore_dicts:
        return {_tostring(key, ignore_dicts=True): _tostring(value, ignore_dicts=True) for key, value in data.iteritems()}
    # if it's anything else, return it in its original form
    return data

def consume_messages(topics):

	# stop iterations after 20 mins
	consumer = KafkaConsumer(bootstrap_servers=[port])
	consumer.subscribe(topics)
	count = 0
	print port
	for message in consumer:
		# check for stopping input
		signal.signal(signal.SIGINT , signal_handler)

		incoming_message = json.loads(message.value,object_hook=_tostring)

		incoming_topic = message.topic

		# round trip for consistent values
		# trip_dist = round(incoming_message["trip_distance"][str(0)],2)
		count = count + 1
		print "--------------"
		print incoming_message
		print "--------------"
		
		new_entry = {"pick_location": {
				          "lat":  float(incoming_message["pickup_latitude"]),
				          "lon":  float(incoming_message["pickup_longitude"])
				        },
			        "drop_location": {
				          "lat":  float(incoming_message["dropoff_latitude"]),
				          "lon":  float(incoming_message["dropoff_longitude"])
				        },
			        "trip_distance": float(incoming_message["trip_distance"]),
			        "timestamp": incoming_message["timestamp"]		        
				      }
		print new_entry
		es.index(index=incoming_topic, doc_type=incoming_topic[:-1], id=str(count), body =new_entry)


if __name__ == '__main__':

	# load configuration parameters
	config_path = 'config/stream_consumer_config.yml'
	config = yaml_loader(config_path)

	# initialize parameters
	port = config['port']
	topics = config['topics']

	# load mapping
	mapping = json_loader('config/stream_elasticsearch_config.json')
	
	# initialize elasticsearch
	es = Elasticsearch()
	print "subscribing elastic search to the following topics: "+str(topics)

	# create index
	for index in topics:
		es.indices.create(index=index, ignore=400, body=mapping[index])

	# consume messages from Kafka
	consume_messages(topics)




