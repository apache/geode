## 5 Minutes Quick Start Guide

In this quick start guide, you will learn how to use Spark shell to test Spark
GemFire Connector functionalities.

### Prerequisites

Before you start, you should have basic knowledge of GemFire and Apache Spark. 
Please refer to [GemFire Documentation](http://gemfire.docs.pivotal.io/latest/userguide/index.html)
and [Spark Documentation](https://spark.apache.org/docs/latest/index.html) for
the details. If you are new to GemFire, this 
[tutorial](http://gemfire.docs.pivotal.io/latest/userguide/index.html#getting_started/gemfire_tutorial/chapter_overview.html)
is a good starting point.

You need 2 terminals to follow along, one for GemFire `gfsh`, and one for Spark shell. Set up Jdk 1.7 on both of them.

### GemFire `gfsh` terminal
In this terminal, start GemFire cluster, deploy Connector's gemfire-function jar, and create demo regions.

Set up environment variables:
```
export JAVA_HOME=<path to JAVA installation>
export GEMFIRE=<path to GemFire installation>
export CONNECTOR=<path to Spark GemFire Connector project (parent dir of this file)>
export CLASSPATH=$CLASSPATH:$GEMFIRE/lib/locator-dependencies.jar:$GEMFIRE/lib/server-dependencies.jar:$GEMFIRE/lib/gfsh-dependencies.jar
export PATH=$PATH:$GEMFIRE/bin
export GF_JAVA=$JAVA_HOME/bin/java
```

Start GemFire cluster with 1 locator and 2 servers:
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

Deploy Connector's gemfire-function jar (`gemfire-functions_2.10-0.5.0.jar`):
```
gfsh>deploy --jar=<path to connector project>/gemfire-functions/target/scala-2.10/gemfire-functions_2.10-0.5.0.jar
```

### Spark shell terminal
In this terminal, setup Spark environment, and start Spark shell.

Set GemFire locator property in Spark configuration: add 
following to `<spark-dir>/conf/spark-defaults.conf`:
```
spark.gemfire.locators=localhost[55221]
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
bin/spark-shell --master local[*] --jars $CONNECTOR/gemfire-spark-connector/target/scala-2.10/gemfire-spark-connector_2.10-0.5.0.jar,$GEMFIRE/lib/server-dependencies.jar
```

Check GemFire locator property in the Spark shell:
```
scala> sc.getConf.get("spark.gemfire.locators")
res0: String = localhost[55221]
```

In order to enable GemFire specific functions, you need to import 
`io.pivotal.gemfire.spark.connector._`
```
scala> import io.pivotal.gemfire.spark.connector._
import io.pivotal.gemfire.spark.connector._
```

### Save Pair RDD to GemFire
In the Spark shell, create a simple pair RDD and save it to GemFire:
```
scala> val data = Array(("1", "one"), ("2", "two"), ("3", "three"))
data: Array[(String, String)] = Array((1,one), (2,two), (3,three))

scala> val distData = sc.parallelize(data)
distData: org.apache.spark.rdd.RDD[(String, String)] = ParallelCollectionRDD[0] at parallelize at <console>:14

scala> distData.saveToGemfire("str_str_region")
15/02/17 07:11:54 INFO DAGScheduler: Job 0 finished: runJob at GemFireRDDFunctions.scala:29, took 0.341288 s
```

Verify the data is saved in GemFile using `gfsh`:
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

### Save Non-Pair RDD to GemFire 
Saving non-pair RDD to GemFire requires an extra function that converts each 
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

### Expose GemFire Region As RDD
The same API is used to expose both replicated and partitioned region as RDDs. 
```
scala> val rdd = sc.gemfireRegion[String, String]("str_str_region")
rdd: io.pivotal.gemfire.spark.connector.rdd.GemFireRDD[String,String] = GemFireRDD[2] at RDD at GemFireRDD.scala:19

scala> rdd.foreach(println)
(1,one)
(3,three)
(2,two)

scala> val rdd2 = sc.gemfireRegion[Int, String]("int_str_region")
rdd2: io.pivotal.gemfire.spark.connector.rdd.GemFireRDD[Int,String] = GemFireRDD[3] at RDD at GemFireRDD.scala:19

scala> rdd2.foreach(println)
(2,ab)
(1,a)
(3,abc)
```
Note: use the right type of region key and value, otherwise you'll get
ClassCastException. 


Next: [Connecting to GemFire](3_connecting.md)


