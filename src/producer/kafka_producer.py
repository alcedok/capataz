"""
=======================================
Simulate Kafka Producer from local file  
=======================================
@author: kevin_alcedo
"""
# print(__doc__)
from kafka import KafkaProducer
import sys
from datetime import datetime
import time
import pandas as pd
import yaml
import sys
import signal
import random
import os.path

def yaml_loader(yaml_file):
	# os.path.dirname(os.path.realpath(__file__))
	with open(yaml_file) as yml:
		config = yaml.load(yml)
	return config

def load_csv(path_to_data,data_columns):
	nchunk_data = 1
	# read csv as a pandas DataFrame, filtering invalid data and using only the specified columns
	data_file = pd.read_csv(path_to_data,error_bad_lines=False,sep=',',header=0, iterator=True,chunksize = nchunk_data)
	return data_file

def signal_handler(signal,frame):
	print "--"
	print "--- kafka producer has been halted ---"
	print "--"
	sys.exit(0)

if __name__ == '__main__':

	# path to config file
	config_source = "config/producer_config.yml"
	
	# load configuration parameters
	config = yaml_loader(config_source)

	# initialize parameters
	topics = config['topics']
	time_buffer = config['time_buffer']
	data_source = config['data_file_name']
	data_columns = config['data_column_keys']
	port = config['port']

	# path to data file
	path_to_data = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..',data_source))
	# load data from directory 
	data_file = load_csv(path_to_data,data_columns)
	# create a producer object for the given ip
	producer = KafkaProducer(bootstrap_servers=[port])
	# producer = KafkaProducer(bootstrap_servers=ip_address)

	# run unless interrupted
	while True:
		# iterate through DataFrame every second
		for chunk in data_file:

			# check for stopping input
			signal.signal(signal.SIGINT , signal_handler)

			# append current time to Pandas DataFrame
			current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
			# current_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
			time_dataframe  = pd.DataFrame({'timestamp':[current_time]})

			# select a random topic
			rand = random.randint(0,len(topics)-1) 
			print topics[rand]
		
			# convert DataFrame to json string 
			message = (chunk.join(time_dataframe)).to_json(double_precision=15,orient='records')

			
			print message.strip('[]').splitlines()[0]
			# send messages
			producer.send(topics[rand], value = message.strip('[]').splitlines()[0])
			#print topics[rand]

			# block until all async messages are sent
			producer.flush()

			# buffer time 
			time.sleep(time_buffer)
