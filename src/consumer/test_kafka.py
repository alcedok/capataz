from kafka import KafkaConsumer
import sys
import signal
import yaml
import json


def yaml_loader(yaml_file):
	# os.path.dirname(os.path.realpath(__file__))
	with open(yaml_file) as yml:
		config = yaml.load(yml)
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
	consumer = KafkaConsumer(bootstrap_servers=[port],consumer_timeout_ms=1200000)
	consumer.subscribe(topics)
	for message in consumer:
		# check for stopping input
		signal.signal(signal.SIGINT , signal_handler)

		incoming_message = json.loads(message.value,object_hook=_tostring)

		incoming_topic = message.topic
		print incoming_topic
		print incoming_message

if __name__ == '__main__':
	# load configuration parameters
	config_path = 'config/consumer_config.yml'
	config = yaml_loader(config_path)

	# initialize parameters
	port = config['port']
	topics = config["topics"]
	
	print "subscribing to the following topics: "+str(topics)

	# consume messages from Kafka
	consume_messages(topics)