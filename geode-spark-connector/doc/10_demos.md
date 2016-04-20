## About The Demos
The Spark Geode Connector contains basic demos, as samples, in both Scala
and Java.

 - Read Geode region to Spark as a RDD (`RegionToRDDJavaDemo.java`)
 - Write Spark pair RDD to Geode (`PairRDDSaveJavaDemo.java`)
 - Write Spark non-pair RDD to Geode (`RDDSaveJavaDemo.java`)
 - Read OQL query result as Spark DataFrame (OQLJavaDemo.java)
 - Network stateful word count (NetworkWordCount.scala)

### Requirements
Running the demo requires a Geode Cluster. This can be a one 
node or multi-node cluster.

Here are the commands that start a two-node Geode cluster on localhost:
First set up environment variables:
```
export JAVA_HOME=<path to JAVA installation>
export GEODE=<path to Geode installation>
export CONNECTOR=<path to Connector project>
export CLASSPATH=$CLASSPATH:$GEODE/lib/locator-dependencies.jar:$GEODE/lib/server-dependencies.jar:$GEODE/lib/gfsh-dependencies.jar
export PATH=$PATH:$GEODE/bin
export GF_JAVA=$JAVA_HOME/bin/java

Now run gfsh and execute the commands:
$ cd <path to test Geode cluster instance location>
$ mkdir locator server1 server2
$ gfsh
gfsh> start locator --name=locator
gfsh> start server --name=server1 --server-port=40411
gfsh> start server --name=server2 --server-port=40412 
```

In order to run the Demos, you need to create the following regions
via `gfsh`:
```
gfsh> create region --name=str_str_region --type=REPLICATE --key-constraint=java.lang.String --value-constraint=java.lang.String
gfsh> create region --name=str_int_region --type=PARTITION --key-constraint=java.lang.String --value-constraint=java.lang.Integer
```

And deploy Geode functions required by the Spark Geode Connector:
```
gfsh> deploy --jar=<path to connector project>/geode-functions/target/scala-2.10/geode-functions_2.10-0.5.0.jar
```

### Run simple demos
This section describes how to run `RDDSaveJavaDemo.java`, 
`PairRDDSaveJavaDemo.java` and `RegionToRDDJavaDemo.java`:
```
export SPARK_CLASSPATH=$CONNECTOR/geode-spark-connector/target/scala-2.10/geode-spark-connector_2.10-0.5.0.jar:$GEODE/lib/server-dependencies.jar

cd <spark 1.3 dir>
bin/spark-submit --master=local[2] --class demo.RDDSaveJavaDemo $CONNECTOR/geode-spark-demos/basic-demos/target/scala-2.10/basic-demos_2.10-0.5.0.jar locatorHost[port]

bin/spark-submit --master=local[2] --class demo.PairRDDSaveJavaDemo $CONNECTOR/geode-spark-demos/basic-demos/target/scala-2.10/basic-demos_2.10-0.5.0.jar locatorHost[port]

bin/spark-submit --master=local[2] --class demo.RegionToRDDJavaDemo $CONNECTOR/geode-spark-demos/basic-demos/target/scala-2.10/basic-demos_2.10-0.5.0.jar locatorHost[port]
```

### Run stateful network word count
This demo shows how to save DStream to Geode. To run the demo, open 3 Terminals:

**Terminal-1**, start net cat server:
```
$ nc -lk 9999
```

**Terminal-2**, start word count Spark app: 
```
bin/spark-submit --master=local[2] demo.NetworkWordCount $CONNECTOR/geode-spark-demos/basic-demos/target/scala-2.10/basic-demos_2.10-0.5.0.jar localhost 9999 locatorHost:port`
```

Switch to Terminal-1, type some words, and hit `enter` or `return` key, then check word count at **Terminal-3**, which has `gfsh` connected to the Geode cluster:
```
gfsh> query --query="select key, value from /str_int_region.entrySet" 
```

### Shutdown Geode cluster at the end
Use following command to shutdown the Geode cluster after playing with
the demos:
```
gfsh> shutdown --include-locators=true
```

