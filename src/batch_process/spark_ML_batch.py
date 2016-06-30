"""
=======================================
Spark Batch Process
=======================================
Summary:
- Extract CSV from HDFS
- Convert CVS to Spark DataFrame
- Filter invalid data
- Feature engineering
- Train model
- Push prediction to Kafka

"""
# print(__doc__)

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType,StructType,IntegerType,StructField,FloatType,TimestampType,DateType,DoubleType
from pyspark.sql.functions import hour,udf,col,minute
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
from datetime import datetime
from  kafka_message_sender import KafkaMessageSender

def quiet_logs(sc):
	# remove INFO log on terminal
	logger = sc._jvm.org.apache.log4j
	logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
	logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

def set_up_spark():

	######################
	#
	# initialize spark
	#
	######################

	conf = SparkConf()
	conf.setAppName("Spark Test")
	conf.set('spark.shuffle.io.preferDirectBufs','false')
	sc = SparkContext(conf = conf)
	quiet_logs(sc)
	sqlContext = SQLContext(sc)
	return sqlContext,sc

def time_delta_minutes(time_start,time_end):
	time_start = datetime.strptime(time_start,'%Y-%m-%d %H:%M:%S')
	time_end = datetime.strptime(time_end,'%Y-%m-%d %H:%M:%S')
	return (time_end-time_start).total_seconds() / 60

def spark_process(sqlContext, sc, validate, path_to_file):

	######################
	#
	# HDFS to DataFrame 
	#
	######################

	
	## all fields:
	#  ['vendor_id', 'pickup_datetime', 'dropoff_datetime', 'passenger_count', 'trip_distance', 
	#   'pickup_longitude', 'pickup_latitude', 'rate_code', 'store_and_fwd_flag', 'dropoff_longitude', 
	#   'dropoff_latitude', 'payment_type', 'fare_amount', 'surcharge', 'mta_tax', 'tip_amount', 
	#   'tolls_amount', 'total_amount']

	# columns to select
	feature_columns = [1,2,3,5,6,9,10]

	# read file and convert to DataFrame
	# dataframe = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(path_to_file).cache()
	customSchema = StructType([
    							StructField("vendor_id", StringType(), True),
							    StructField("pickup_datetime", TimestampType(), True),
							    StructField("dropoff_datetime", TimestampType(), True),
							    StructField("passenger_count", StringType(), True),
							    StructField("trip_distance", StringType(), True),
							    StructField("pickup_longitude", DoubleType(), True),
							    StructField("pickup_latitude", DoubleType(), True),
							    StructField("rate_code", StringType(), True),
							    StructField("store_and_fwd_flag", StringType(), True),
							    StructField("dropoff_longitude", DoubleType(), True),
							    StructField("dropoff_latitude", DoubleType(), True),
							    StructField("payment_type", StringType(), True),
							    StructField("fare_amount", StringType(), True),
							    StructField("surcharge", StringType(), True),
							    StructField("mta_tax", StringType(), True),
							    StructField("tip_amount", StringType(), True),
							    StructField("tolls_amount", StringType(), True),
							    StructField("total_amount", StringType(), True)
							    ])

	dataframe = sqlContext.read.format('com.databricks.spark.csv').options(header='true', schema = customSchema).load(path_to_file)
	# create dataframe with selected columns
	dataframe = dataframe.select(*(dataframe.columns[n] for n in feature_columns))
	
	# this number does not include the header
	# number_of_trips = dataframe.count()

	sqlContext.clearCache()
	######################
	#
	# Preprocess data 
	#
	######################

	# filter rows with null fields
	# if passenger count is missing assign it a value of 1
	# filter invalid location: keep only areas near NYC
	dataframe = dataframe.na.drop(how='any',subset=['pickup_datetime','dropoff_datetime','pickup_longitude','pickup_latitude','dropoff_longitude','dropoff_latitude']) \
						.fillna(1,subset=["passenger_count"])     \
						.filter(dataframe.pickup_latitude>40.0)   \
						.filter(dataframe.pickup_latitude<41.0)   \
						.filter(dataframe.pickup_longitude<-73.0) \
						.filter(dataframe.pickup_longitude>-74.0) \
						.filter(dataframe.dropoff_latitude>40.0)  \
						.filter(dataframe.dropoff_latitude<41.0)  \
						.filter(dataframe.dropoff_longitude<-73.0)\
						.filter(dataframe.dropoff_longitude>-74.0)


	######################
	#
	# features engineering
	#
	######################

	# create new column based on time-delta (minutes)
	# convert pickup-datetime column to hour
		
	time_delta_udf = udf(time_delta_minutes,FloatType())

	dataframe = dataframe.withColumn('time_delta', time_delta_udf(dataframe.pickup_datetime,dataframe.dropoff_datetime)) \
						 .withColumn('pick_up_hour', hour(dataframe.pickup_datetime))

 	dataframe = dataframe.select(dataframe.pick_up_hour,    \
 								dataframe.passenger_count.cast("integer"),  \
								dataframe.pickup_longitude.cast("double"), \
								dataframe.pickup_latitude.cast("double"),  \
								dataframe.dropoff_longitude.cast("double"),\
								dataframe.dropoff_latitude.cast("double"), \
								dataframe.time_delta.cast("double"))

 	dataframe = dataframe.filter(dataframe.time_delta > 1.0).cache()


 	# split dataframe into feature and label vector
	# create feature vectors and labels for model training
	feature_assembler = VectorAssembler(inputCols = ['pick_up_hour','pickup_longitude','pickup_latitude','dropoff_longitude','dropoff_latitude'],outputCol = 'features')

	transformed = feature_assembler.transform(dataframe)
	vector_dataframe = transformed.select(col("time_delta").alias("label"),col("features")).cache()

	######################
	#
	# train model
	#
	######################

	if validate:

		################################
		#
		# validate model on 60/40 split
		#
		################################

		# split 
		training, test = vector_dataframe.randomSplit([0.6, 0.4], seed=0)

		decision_tree_reg = DecisionTreeRegressor(maxDepth=12,maxBins=25)
		model = decision_tree_reg.fit(training)

		train_pred = model.transform(training)
		test_pred = model.transform(test)

		evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2")
		r2_train = evaluator.evaluate(train_pred)

		evaluator_test = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2")
		r2_test = evaluator_test.evaluate(test_pred)

		output = test_pred.select("prediction", "label", "features")

		return output, r2_test, r2_train
	
	else:

		###################
		#
		# train on all data
		#
		###################

		decision_tree_reg = DecisionTreeRegressor(maxDepth=12,maxBins=25)
		model = decision_tree_reg.fit(vector_dataframe)

		predictions = model.transform(vector_dataframe)

		output = predictions.select("prediction", "label", "features")

		###########################
		#
		# process to send to Kafka
		#
		###########################

		schema = StructType([StructField("prediction_mins", FloatType(), True),
							StructField("pick_up_hour", IntegerType(), True),
							StructField("pickup_longitude", DoubleType(), True),
							StructField("pickup_latitude", DoubleType(), True),
							StructField("dropoff_longitude", DoubleType(), True),
							StructField("dropoff_latitude", DoubleType(), True)])

		features_from_predictions = output.map(lambda row: (float(row.prediction),int(row.features[0]),float(row.features[1]),float(row.features[2]),float(row.features[3]),float(row.features[4]) ) ).collect()
		dataframe_from_prediction_vector = sqlContext.createDataFrame(features_from_predictions,schema)

		return dataframe_from_prediction_vector

if __name__ == '__main__':

	# CSV data file from HDFS
	path_to_file = "hdfs://ec2-52-205-3-118.compute-1.amazonaws.com:9000/data/test/test_data_30.csv"
	# set up Spark Configs
	sqlContext, sc = set_up_spark()

	## configure Kafka Producer
	config_source = "config/producer_config.yml"
	kafka_sender = KafkaMessageSender(config_source)

	validate = False
	if validate:
		# run Spark process and validate model on 40/60 split
		model_output,r2_test, r2_train = spark_process(sqlContext, sc, validate,path_to_file)
		print("-----(r2) on train data = " + str(r2_train) + " -----")
		print("-----(r2) on test data = "  + str(r2_test)  + " -----")
	else: 
		# run Spark process
		model_output = spark_process(sqlContext, sc, validate,path_to_file)
		
		model_output.show(n=5)

		messages = model_output.toJSON(use_unicode=False).collect()
		# print messages
		kafka_sender.send_message(messages)



