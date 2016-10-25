## Saving DStream to Geode
Spark Streaming extends the core API to allow high-throughput, fault-tolerant
stream processing of live data streams.  Data can be ingested from many 
sources such as Akka, Kafka, Flume, Twitter, ZeroMQ, TCP sockets, etc. 
Results can be stored in Geode.

### A Simple Spark Streaming App: Stateful Network Word Count

Create a `StreamingContext` with a `SparkConf` configuration
```
val ssc = new StreamingContext(sparkConf, Seconds(1))
```

Create a DStream that will connect to net cat server `host:port`
```
val lines = ssc.socketTextStream(host, port)
```

Count each word in each batch
```
val words = lines.flatMap(_.split(" "))
val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _)
```

Use `updateStateByKey` to maintain a running count of each word seen in a text
data stream. Here, the running count is the state and it is an integer. We 
define the update function as
```
val updateFunc = (values: Seq[Int], state: Option[Int]) => {
  val currentCount = values.foldLeft(0)(_ + _)
  val previousCount = state.getOrElse(0)
  Some(currentCount + previousCount)
}
```

This is applied on a DStream containing words (say, the pairs DStream containing
`(word, 3)` pairs in the earlier example
```
val runningCounts = wordCounts.updateStateByKey[Int](updateFunction _)
```

Print a few of the counts to the console. Start the computation.
```
runningCounts.print()
ssc.start()
ssc.awaitTermination() // Wait for the computation to terminate
```

#### Spark Streaming With Geode
Now let's save the running word count to Geode region `str_int_region`, which 
simply replace print() with saveToGeode():

```
import org.apache.geode.spark.connector.streaming._
runningCounts.saveToGeode("str_int_region")
```

You can use the version of saveToGeode that has the parameter `GeodeConnectionConf`:
```
runningCounts.saveToGeode("str_int_region", connConf)
```

See [Spark Streaming Programming Guide]
(http://spark.apache.org/docs/latest/streaming-programming-guide.html) for 
more details about Sarpk streaming programming.


Next: [Geode OQL](8_oql.md)
