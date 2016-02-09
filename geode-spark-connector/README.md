#Spark Geode Connector

Spark Geode Connector let's you connect Spark to Geode, expose Geode regions as Spark 
RDDs, save Spark RDDs to Geode and execute Geode OQL queries in your Spark applications
and expose the results as DataFrames.

##Features:

 - Expose Geode region as Spark RDD with Geode server-side filtering
 - RDD join and outer join Geode region
 - Save Spark RDD to Geode
 - Save DStream to Geode
 - Execute Geode OQL and return DataFrame

##Version and Compatibility

Spark Geode Connector supports Spark 1.3.

##Documentation
 - [Building and testing](doc/1_building.md)
 - [Quick start](doc/2_quick.md)
 - [Connect to Geode](doc/3_connecting.md)
 - [Loading data from Geode](doc/4_loading.md)
 - [RDD Join and Outer Join Geode Region](doc/5_rdd_join.md)
 - [Saving RDD to Geode](doc/6_save_rdd.md)
 - [Saving DStream to Geode](doc/7_save_dstream.md)
 - [Geode OQL](doc/8_oql.md)
 - [Using Connector in Java](doc/9_java_api.md)
 - [About the demos](doc/10_demos.md)

##License: Apache License 2.0

