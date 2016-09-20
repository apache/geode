## 5 Minutes Quick Start Guide

In this quick start guide, you will learn how to use Spark shell to test Spark
Geode Connector functionalities.

### Prerequisites

Before you start, you should have basic knowledge of Geode and Spark. 
Please refer to [Geode Documentation](http://geode.incubator.apache.org/docs/)
and [Spark Documentation](https://spark.apache.org/docs/latest/index.html) for
details. If you are new to Geode, this 
[Quick Start Guide](http://geode-docs.cfapps.io/docs/getting_started/15_minute_quickstart_gfsh.html)
is a good starting point.

You need 2 terminals to follow along, one for Geode shell `gfsh`, and one for Spark shell. Set up Jdk 1.7 on both of them.

### Geode `gfsh` terminal
In this terminal, start Geode cluster, deploy Spark Geode Connector's geode-function jar, and create demo regions.

Set up environment variables:
```
export JAVA_HOME=<path to JAVA installation>
export GEODE=<path to GEODE installation>
export CONNECTOR=<path to Spark GEODE Connector project (parent dir of this file)>
export CLASSPATH=$CLASSPATH:$GEODE/lib/locator-dependencies.jar:$GEODE/lib/server-dependencies.jar:$GEODE/lib/gfsh-dependencies.jar
export PATH=$PATH:$GEODE/bin
export GF_JAVA=$JAVA_HOME/bin/java
```

Start Geode cluster with 1 locator and 2 servers:
```
gfsh
gfsh>start locator --name=locator1 --port=55221
gfsh>start server --name=server1 --locators=localhost[55221] --server-port=0
gfsh>start server --name=server2 --locators=localhost[55221] --server-port=0
```

Then create two demo regions:
```
gfsh>create region --name=str_str_region --type=PARTITION --key-constraint=java.lang.String --value-constraint=java.lang.String
gfsh>create region --name=int_str_region --type=PARTITION --key-constraint=java.lang.Integer --value-constraint=java.lang.String
```

Deploy Spark Geode Connector's geode-function jar (`geode-functions_2.10-0.5.0.jar`):
```
gfsh>deploy --jar=<path to connector project>/geode-functions/target/scala-2.10/geode-functions_2.10-0.5.0.jar
```

### Spark shell terminal
In this terminal, setup Spark environment, and start Spark shell.

Set Geode locator property in Spark configuration: add 
following to `<spark-dir>/conf/spark-defaults.conf`:
```
spark.geode.locators=localhost[55221]
```
Note:
 - if the file doesn't exist, create one. 
 - replace string `localhost[55221]` with your own locator host and port.

By default, Spark shell output lots of info log, if you want to
turn off info log, change `log4j.rootCategory` to `WARN, console`
in file `<spark dir>/conf/conf/log4j.properties`:
```
log4j.rootCategory=WARN, console
```
if file `log4j.properties` doesn't exist, copy `log4j.properties.template`
under the same directory to `log4j.properties` and update the file.

Start spark-shell:
```
bin/spark-shell --master local[*] --jars $CONNECTOR/geode-spark-connector/target/scala-2.10/geode-spark-connector_2.10-0.5.0.jar,$GEODE/lib/server-dependencies.jar
```

Check Geode locator property in the Spark shell:
```
scala> sc.getConf.get("spark.geode.locators")
res0: String = localhost[55221]
```

In order to enable Geode specific functions, you need to import 
`org.apache.geode.spark.connector._`
```
scala> import org.apache.geode.spark.connector._
import org.apache.geode.spark.connector._
```

### Save Pair RDD to Geode
In the Spark shell, create a simple pair RDD and save it to Geode:
```
scala> val data = Array(("1", "one"), ("2", "two"), ("3", "three"))
data: Array[(String, String)] = Array((1,one), (2,two), (3,three))

scala> val distData = sc.parallelize(data)
distData: org.apache.spark.rdd.RDD[(String, String)] = ParallelCollectionRDD[0] at parallelize at <console>:14

scala> distData.saveToGemfire("str_str_region")
15/02/17 07:11:54 INFO DAGScheduler: Job 0 finished: runJob at GemFireRDDFunctions.scala:29, took 0.341288 s
```

Verify the data is saved in Geode using `gfsh`:
```
gfsh>query --query="select key,value from /str_str_region.entries"

Result     : true
startCount : 0
endCount   : 20
Rows       : 3

key | value
--- | -----
1   | one
3   | three
2   | two

NEXT_STEP_NAME : END
```

### Save Non-Pair RDD to Geode 
Saving non-pair RDD to Geode requires an extra function that converts each 
element of RDD to a key-value pair. Here's sample session in Spark shell:
```
scala> val data2 = Array("a","ab","abc")
data2: Array[String] = Array(a, ab, abc)

scala> val distData2 = sc.parallelize(data2)
distData2: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[0] at parallelize at <console>:17

scala> distData2.saveToGemfire("int_str_region", e => (e.length, e))
[info 2015/02/17 12:43:21.174 PST <main> tid=0x1]
...
15/02/17 12:43:21 INFO DAGScheduler: Job 0 finished: runJob at GemFireRDDFunctions.scala:52, took 0.251194 s
```

Verify the result with `gfsh`:
```
gfsh>query --query="select key,value from /int_str_region.entrySet"

Result     : true
startCount : 0
endCount   : 20
Rows       : 3

key | value
--- | -----
2   | ab
3   | abc
1   | a

NEXT_STEP_NAME : END
```

### Expose Geode Region As RDD
The same API is used to expose both replicated and partitioned region as RDDs. 
```
scala> val rdd = sc.geodeRegion[String, String]("str_str_region")
rdd: org.apache.geode.spark.connector.rdd.GemFireRDD[String,String] = GemFireRDD[2] at RDD at GemFireRDD.scala:19

scala> rdd.foreach(println)
(1,one)
(3,three)
(2,two)

scala> val rdd2 = sc.geodeRegion[Int, String]("int_str_region")
rdd2: org.apache.geode.spark.connector.rdd.GemFireRDD[Int,String] = GemFireRDD[3] at RDD at GemFireRDD.scala:19

scala> rdd2.foreach(println)
(2,ab)
(1,a)
(3,abc)
```
Note: use the right type of region key and value, otherwise you'll get
ClassCastException. 


Next: [Connecting to Geode](3_connecting.md)


