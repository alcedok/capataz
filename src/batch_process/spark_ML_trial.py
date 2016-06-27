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
- Push prediction to Elasticsearch

"""
# print(__doc__)
# from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, functions
from pyspark.sql.types import StringType,StructType,IntegerType,StructField,FloatType,TimestampType
from pyspark.sql.functions import hour,udf, col, monotonically_increasing_id,minute

# from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD,LinearRegressionModel
from pyspark.ml import Pipeline
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import VectorAssembler,StringIndexer,IndexToString,VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator

import itertools

from kafka import KafkaConsumer

from pygeohash import encode


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

def spark_process(sqlContext, sc):

	######################
	#
	# HDFS to DataFrame 
	#
	######################

	# CSV data file from HDFS
	path_to_file = "hdfs://ec2-52-205-3-118.compute-1.amazonaws.com:9000/data/test/test_data_batch.csv"

	## all fields:
	#  ['vendor_id', 'pickup_datetime', 'dropoff_datetime', 'passenger_count', 'trip_distance', 
	#   'pickup_longitude', 'pickup_latitude', 'rate_code', 'store_and_fwd_flag', 'dropoff_longitude', 
	#   'dropoff_latitude', 'payment_type', 'fare_amount', 'surcharge', 'mta_tax', 'tip_amount', 
	#   'tolls_amount', 'total_amount']

	# columns to select
	feature_columns = [1,5,6]

	# read file and convert to DataFrame
	# dataframe = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(path_to_file).cache()
	customSchema = StructType([
    							StructField("vendor_id", StringType(), True),
							    StructField("pickup_datetime", TimestampType(), True),
							    StructField("dropoff_datetime", StringType(), True),
							    StructField("passenger_count", StringType(), True),
							    StructField("trip_distance", StringType(), True),
							    StructField("pickup_longitude", FloatType(), True),
							    StructField("pickup_latitude", FloatType(), True),
							    StructField("rate_code", StringType(), True),
							    StructField("store_and_fwd_flag", StringType(), True),
							    StructField("dropoff_longitude", StringType(), True),
							    StructField("dropoff_latitude", StringType(), True),
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
	# filter invalid data 
	#
	######################

	# filter rows with empty fields
	dataframe = dataframe.na.drop(how='any',subset=['pickup_datetime','pickup_longitude','pickup_latitude']).cache()

	#filter invalid location
	#keep only areas near NYC
	dataframe = dataframe.filter(dataframe.pickup_latitude>40.0).filter(dataframe.pickup_latitude<41.0).filter(dataframe.pickup_longitude<-73.0).filter(dataframe.pickup_longitude>-74.0)
	dataframe = dataframe.select(monotonically_increasing_id().alias('row_id'),dataframe.pickup_datetime,dataframe.pickup_latitude,dataframe.pickup_longitude).cache()

	######################
	#
	# features engineering
	#
	######################

	# geohash pick_up locations
	pgh_func = udf(encode,StringType())

	# create a dataframe that contains the geohashes for the given coordinates
	geohashes_dataframe = dataframe.select( pgh_func(dataframe.pickup_latitude,dataframe.pickup_longitude).alias('geohash'))
	# unique_hashes_dataframe = feature_dataframe.select('geohash').distinct()

	# convert string geohashes to numerical ids, geo_id
	indexer = StringIndexer(inputCol="geohash", outputCol="geo_id",handleInvalid="skip")
	indexed = indexer.fit(geohashes_dataframe).transform(geohashes_dataframe).cache()
	indexed = indexed.select(monotonically_increasing_id().alias('row_id'),indexed.geo_id.cast("float")).replace(0.0,-1.0,'geo_id').cache

	#dataframe containing both geo_id and hours
	feature_dataframe = dataframe.select(dataframe.row_id,minute(dataframe.pickup_datetime).alias('minute').cast("float"),hour(dataframe.pickup_datetime).alias('hour').cast("float")).join(indexed.select(indexed.geo_id,indexed.row_id),how='outer',on="row_id").replace(0.0,-1.0,'hour').cache()

	#training labels
	#count number of pickups for a geo_id and hour pair
	labels = feature_dataframe.map(lambda row: (row.row_id,str(row.hour)+','+str(row.geo_id),) ).toDF(['row_id','hour_geo']).cache()

	# dictionary of geo_id and counts
	label_counts = labels.groupby([labels.hour_geo]).count().rdd.collectAsMap()

	# map each row in dataframe with the given counts
	labels_and_counts = labels.map(lambda row: (row.row_id , row.hour_geo , float(label_counts[row.hour_geo]) ) ).toDF(['row_id','hour_geo','counts'])
	# labels_and_counts.show()

	# join counts with original feature dataframe 
	model_input = feature_dataframe.join(labels_and_counts.select(labels_and_counts.row_id,labels_and_counts.hour_geo,labels_and_counts.counts),how='right_outer',on="row_id")
	# model_input = model_input.select(model_input.hour,model_input.geo_id,model_input.counts)

	# create feature vectors and labels for model training
	feature_assembler = VectorAssembler(inputCols = ['geo_id','hour'],outputCol = 'features')
	transformed = feature_assembler.transform(model_input).cache()
	vector_dataframe = transformed.select(col("counts").alias("label"),col("features")).cache()

	#save this output for other training
	# vector_dataframe.toDF().show(n=20)
	# # (vector_dataframe.toDF(['features','labels'])).write.format('com.databricks.spark.csv').save('training_data.csv')

	######################
	#
	# train model
	#
	######################

	# split 
	training, test = vector_dataframe.randomSplit([0.6, 0.4], seed=0)
	# vector_dataframe_count = vector_dataframe.count()
	# print training.collect()

	# train_count = training.count()
	# test_count = test.count()
	# print "training: ",train_count
	# print 'test: ',test_count

	decision_tree_reg = DecisionTreeRegressor(maxDepth=10)
	model = decision_tree_reg.fit(training)
	predictions = model.transform(training)
	test_pred = model.transform(test)

	evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2")
	r2_train = evaluator.evaluate(predictions)

	evaluator_test = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2")
	r2_test = evaluator_test.evaluate(test_pred)

	# predictions.select("prediction", "label", "features").show(30)
	# test_pred.select("prediction", "label", "features").show(30)

	######################
	#
	# fit model
	#
	######################

	# features_from_predicitons = predictions.select("features").map(lambda row: (row[0],row[1]) ).cache().toDf(["geo_id","hour"])

	# features_from_predictions = predictions.map(lambda row: Row(row.prediction,row.label,row.features[0],row.features[1]) ).rdd.toDf(["prediciton","label","geo_id","hour"])

	# features_from_predictions = predictions.map(lambda row: (float(row.prediction),float(row.features[0]),float(row.features[1])) ).collect()
	# schema = StructType([StructField("prediciton", FloatType(), True),
	# 					StructField("geo_id", FloatType(), True),
	# 					StructField("hour", FloatType(), True)])

	# features_from_predictions = sqlContext.createDataFrame(features_from_predictions,schema)

	# features_from_predictions = features_from_predictions.toDf(["prediciton","label","geo_id","hour"])
	output = predictions.select("prediction", "features")

	# predict on training set
	# values_and_predictions = (training.map(lambda p: (p.label, model.predict(p.features))))
	# train_error = values_and_predictions.filter(lambda (val,pred): val != pred).count()/float(train_count)
	# MSE = values_and_predictions.map(lambda (v, p): (v - p)**2).reduce(lambda x, y: x + y) / float(train_count)
	# # # predict on testing set
	# values_and_predictions = (test.map(lambda p: (p.label, model.predict(p.features))))
	# test_error = values_and_predictions.filter(lambda (val,pred): val != pred).count()/float(test_count)

	return output, r2_test, r2_train

# def send_message(message):
# 	consumer = KafkaConsumer(bootstrap_servers=[port],consumer_timeout_ms=1200000)
# 	consumer.subscribe(topics)
# 	print('hi')
# 	print message.take(1)
	

if __name__ == '__main__':

	# set up Spark Configs
	sqlContext, sc = set_up_spark()
	# run Spark process and output prediction for each hour,geohash as a dataframe
	model_output,rmse_test, rmse_train = spark_process(sqlContext, sc)
	print("-----Train (r2) on test data = " + str(rmse_train) + " -----")
	print("-----Test  (r2) on test data = " + str(rmse_test)  + " -----")
	model_output.show(n=5)

	# convert dataframe to json message
	# df.toJSON(use_unicode=False).foreach(send_message)

	# path_to_save = "hdfs://ec2-52-205-3-118.compute-1.amazonaws.com:9000/data/prediction.json"
	# model_output.write.json(path_to_save,mode='overwrite')
	
	# df.toJSON(use_unicode=False).foreach(send_message)
	# send json message to Kafka queue


# # split 
# training, test = vector_dataframe.randomSplit([1.0, 1.0], seed=0)
# # vector_dataframe_count = vector_dataframe.count()
# train_count = training.count()
# test_count = test.count()
# # model_input_count = model_input.count()


# # build model
# # model = LinearRegressionWithSGD.train(training,regParam=0.0,intercept=True,regType = "l1",validateData=True)
# model = LinearRegressionModel.train(training,intercept=True)


# # predict on training set
# values_and_predictions = (training.map(lambda p: (p.label, model.predict(p.features))))
# train_error = values_and_predictions.filter(lambda (val,pred): val != pred).count()/float(train_count)
# MSE = values_and_predictions.map(lambda (v, p): (v - p)**2).reduce(lambda x, y: x + y) / float(train_count)
# # # predict on testing set
# # values_and_predictions = (test.map(lambda p: (p.label, model.predict(p.features))))
# # test_error = values_and_predictions.filter(lambda (val,pred): val != pred).count()/float(test_count)

# # # print("-----Testing Error = "  + str(test_error*100) + "-----")
# a = training.collect()
# b = values_and_predictions.collect()
# print "training: ",train_count
# print 'test: ',test_count
# print a
# print b
# print("-----Training Error = " + str(train_error*100) + "-----")
# print("-----Mean Squared Error = " + str(MSE)+"-----")
# # #







# f = file.map(lambda x: x) # without this line the saving will fail because of the RDD casting bug
# df.write.format('com.databricks.spark.csv').save('mycsv.csv')
# vector_dataframe.saveAsNewAPIHadoopFile('hdfs://ec2-52-205-3-118.compute-1.amazonaws.com:9000/output',      # Output directory
#                          'org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat') # Output format class

# place all data in a single partition and save to csv
# df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("mydata.csv")

# path to save output
# path_to_save = "hdfs://ec2-52-205-3-118.compute-1.amazonaws.com:9000/data/prediction.csv"
# df.write.format('com.databricks.spark.csv').save(path_to_save)



# #convert geo_id back to geohash
# converter = IndexToString(inputCol="geo_id", outputCol="geohash")
# # converted = converter.transform(indexed.select(indexed.geo_id).select(converter.geohash)
# converted = converter.transform(indexed.select(indexed.geo_id))
# converted.show()

# output with geohash and hour
# output_dataframe = converted.select(converted.geohash).join(feature_dataframe.select(feature_dataframe.hour),how='right_outer')
# output_dataframe.show()
