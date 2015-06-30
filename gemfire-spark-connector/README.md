#Spark GemFire Connector

Note: GemFire is now an open source project [Geode](http://projectgeode.org).

Spark GemFire Connector let's you connect Spark to GemFire, expose GemFire regions as Spark 
RDDs, save Spark RDDs to GemFire and execute GemFire OQL queries in your Spark applications
and expose results as DataFrames.

##Features:
 - Expose GemFire region as Spark RDD with GemFire server-side filtering
 - RDD join and outer join GemFire region
 - Save Spark RDD to GemFire
 - Save DStream to GemFire
 - Execute GemFire OQL and return DataFrame

##Version and Compatibility
| Connector | Spark | GemFire | Geode |
|-----------|-------|---------|-------|
| 0.5       | 1.3   | 9.0     |   ?   |   

##Download
TBD

##Documentation
 - [Building and testing](doc/1_building.md)
 - [Quick start](doc/2_quick.md)
 - [Connect to GemFire](doc/3_connecting.md)
 - [Loading data from GemFire](doc/4_loading.md)
 - [RDD Join and Outer Join GemFire Region](doc/5_rdd_join.md)
 - [Saving RDD to GemFire](doc/6_save_rdd.md)
 - [Saving DStream to GemFire](doc/7_save_dstream.md)
 - [GemFire OQL](doc/8_oql.md)
 - [Using Connector in Java](doc/9_java_api.md)
 - [About the demos](doc/10_demos.md)
 - [Logging](doc/logging.md)  ???
 - [Security] (doc/security.md) ???


##Community: Reporting bugs, mailing list, contributing

 (TBD)
 
##License: Apache License 2.0

 (TBD) 
