"""
===============
Kafka Producer
===============
@kevin_alcedo
"""

from kafka import KafkaProducer
import yaml

def yaml_loader(yaml_file):
	with open(yaml_file) as yml:
		config = yaml.load(yml)
	return config

class KafkaMessageSender(object):
	
	def __init__(self,config_source):

		self.config_source = config_source
		# config_source = "config/producer_config.yml"

		# load configuration parameters
		config = yaml_loader(self.config_source)

		# initialize parameters
		self.topics = config['topics']
		self.port = config['port']

		self.current_topic = self.topics[0]

		self.producer = KafkaProducer(bootstrap_servers=[self.port])

	def send_message(self,messages):
		for message in messages:
			# self.producer.send(self.current_topic, value = message.strip('[]').splitlines()[0] )
			print message.strip('[]')
			self.producer.send(self.current_topic, value = message.strip('[]') )

			# block until all async messages are sent
			self.producer.flush()