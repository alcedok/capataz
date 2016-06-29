## capataz 
## A Data Analytics Platform for Connected Transportation

### Summary
__capataz__ allows users to explore, analyze and visualize both near-real-time and historical data, composed of geospatial location, distance traveled, number of passengers and time. 

The platform was developed in 3 weeks as part of Insight's Data Engineering Fellowship in NYC. The project allowed me to explore several open-source Big Data technologies, their paradigms and limitations.

__features:__
* Near-real-time data ingestion and processing
* Near-real-time queries on users and vehicles
* Batch processing on historical data
* Pick-up prediction capabilities based on historical data
* User-interface for data exploration and visualization 
* Distributed and scalable 

### Description
capataz (Spanish), translates to overseer/controller:
> _(n)_ a person or thing that directs or regulates something.

__capataz__ was inspired by the rise of connected transportation and the imminent deployment of self-driving vehicles. It is a proof-of-concept platform for users that need to monitor, explore and vizualize vast amounts of streaming data. Such users may be city officials, urban-planners, emergency and mass-transportation services, and upcoming automated services such as delivery and carpooling. Moreover __capataz__ was designed with the intention of being used by Data Scientist in order to incorporate predictive models for both real-time and batch streams of data. 
As it stands __capataz__ is able to process and filter a simulated real-time stream of data which includes geospatial location, distance traveled, number of passengers and time. Moreover, it is able to process and filter large batches of data while simultaneously training a predictive model. The processed data can then be queried, visualized and explored through a user-interface. 

As a proof-of-concept a Decision Tree Regressor was chosen in order to predict number of pick-ups for a given location and time. This information would be valuable for ridesharing and carpooling services that need to optimize their fleet logistics.

The data used for both real-time simulation and batch processing comes from NYC Taxi and Limousine Commission data, which encompasses multiple .csv's totaling 170 GBs. 

### Pipeline

<img src="https://github.com/alcedok/capataz/blob/master/images/pipeline.png" alt="alt text" width="818" height="542">

__Ingestion__: 

__Apache Kafka__ serves as the primary messaging system between technologies within the pipeline. 

* Kafka receives messages from:
	* a _.csv_ that simulates real-time stream (currently ~100 messages per second)
	* a completed Spark Streaming process
	* a completed Spark Batch process

* Kafka sends messages to:
	* a Spark Streaming process
	* a new _.csv_ file on HDFS 
	* Elasticsearch, which are then indexed for querying

__Stream__:

__Apache Spark Streaming__ serves as the near-real-time analytics framework by performing micro-batches on incoming messages from Kafka. 

* Spark Stream performs the following operations:
	* Create a Direct Stream from Kafka, listening for incoming json messages
	* parse json messages into Dataframes
	* Filter invalid GPS coordinates and empty/null entries
	* Send messages back to Kafka, which will be then consumed by Elasticsearch

__Batch__:

__Apache Spark SQL + ML__ is used for processing batch historical data coming from multiple large (2.5GB) .cvs's stored in HDFS.

* Spark SQL+ML performs the following operations:
	* 


__Datastore+Search Engine__: 

__Elasticsearch__ serves as the datastore and search engine, where users, cars and predictions are indexed for later querying. 


__Frontend__:

__Kibana__ is used as the user-interface for Elasticsearch

### How to run
TODO

### Dependencies
TODO
