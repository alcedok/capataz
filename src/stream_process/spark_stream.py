"""
=======================================
Spark Stream Process
=======================================
Summary:
- Extract json from Kafka
- Convert json to Spark DataFrame
- Filter invalid data
- Push to Elasticsearch

"""
# from __future__ import print_function
import json
from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext, Row 
from pyspark.sql.functions import col, when, udf
from pyspark.sql.types import Row,StringType,StructType,IntegerType,StructField
from  kafka_message_sender import KafkaMessageSender

## configure Kafka Producer
# path to config file
config_source = "config/producer_config.yml"
kafka_sender = KafkaMessageSender(config_source)

def quiet_logs(sc):
	# remove INFO log on terminal
	logger = sc._jvm.org.apache.log4j
	logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
	logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

def _tostring(data, ignore_dicts = False):
	#################################
	#
	# convert items in json to string
	#
	#################################

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

def json_loader(json_file):
	with open(json_file) as jsn:
		config = json.load(jsn,object_hook=_tostring)
	return config

def debug(flag,dataframe):
	if flag == True:
		dataframe.show()
	else:
		return

def stream_to_dataframe(row_rdd):
	flag = True

	try:
		if row_rdd is None or row_rdd.isEmpty():
			print "---------rdd empty!---------"
			return 

		else:
			schema = StructType([
						StructField("timestamp", StringType(), True),
						StructField("trip_distance", StringType(), True),
						StructField("pickup_longitude", StringType(), True),
						StructField("pickup_latitude", StringType(), True),
						StructField("dropoff_longitude", StringType(), True),
						StructField("dropoff_latitude", StringType(), True)])
			# convert row RDD to 
			dataframe = sqlContext.createDataFrame(row_rdd,schema)

			######################
			# filter invalid data 
			######################

			# filter invalid hour
			dataframe = dataframe.na.drop(how='any',subset=['timestamp','pickup_longitude','pickup_latitude'])

			#filter invalid location
			#keep only areas near NYC
			filtered_dataframe = dataframe.filter(dataframe.pickup_latitude>40.0).filter(dataframe.pickup_latitude<41.0).filter(dataframe.pickup_longitude<-73.0).filter(dataframe.pickup_longitude>-74.0)
			
			
			# if the dataframe is not empty then send to Kafka
			if not filtered_dataframe.rdd.isEmpty():
				print "---------filtered---------"
				# debug(flag,filtered_dataframe)
				# kafka_sender.current_topic = 'stream_cars'
				# filtered_dataframe.foreach(send_message)
				messages = filtered_dataframe.toJSON(use_unicode=False).collect()
				print messages
				print kafka_sender.current_topic
				kafka_sender.send_message(messages)

	except:
		pass

if __name__ == '__main__':


	## initialize Spark and set configurations
	conf = SparkConf()
	conf.setAppName("Stream Direct from Kafka")
	conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
	sc = SparkContext(conf=conf)
	sqlContext = SQLContext(sc)
	# keep INFO logs off
	quiet_logs(sc)
	ssc = StreamingContext(sc, 2)

	# begin kafka stream
	kafka_stream = KafkaUtils.createDirectStream(ssc, ["cars","users"], {"metadata.broker.list":"localhost:9092"})

	# DStream or kafka message
	parsed = kafka_stream.map(lambda (key,json_stream): json.loads(json_stream,object_hook=_tostring))

	# convert to row RDD
	row_rdd = parsed.map(lambda x: Row(str(x['timestamp']),
										str(x['trip_distance']),
										str(x['pickup_longitude']),
										str(x['pickup_latitude']),
										str(x['dropoff_longitude']),
										str(x['dropoff_latitude'])))
	# row_rdd.pprint(num=1)

	# perform filtering and dataframe conversion 
	row_rdd.foreachRDD(stream_to_dataframe)

	# begin
	ssc.start()
	ssc.awaitTermination()
	

# {"vendor_id":"VTS","pickup_datetime":"2013-05-01 00:04:00","dropoff_datetime":"2013-05-01 00:12:00",
# "passenger_count":1,"trip_distance":1.34,"pickup_longitude":-73.982287,"pickup_latitude":40.772815000000001,
# "rate_code":1,"store_and_fwd_flag":null,"dropoff_longitude":-73.98621,"dropoff_latitude":40.758741999999998,
# "payment_type":"CSH","fare_amount":7,"surcharge":0.5,"mta_tax":0.5,"tip_amount":0,"tolls_amount":0,"total_amount":8,
# "timestamp":"2016\/06\/21 15:50:40.624"}