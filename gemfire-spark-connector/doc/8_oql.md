## GemFire OQL Query
Spark GemFire Connector lets us run GemFire OQL queries in Spark applications
to retrieve data from GemFire. The query result is a Spark DataFrame. Note 
that as of Spark 1.3, SchemaRDD is deprecated. Spark GemFire Connector does
not support SchemaRDD.

An instance of `SQLContext` is required to run OQL query.
```
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
```

Create a `DataFrame` using OQL:
```
val dataFrame = sqlContext.gemfireOQL("SELECT * FROM /CustomerRegion WHERE status = 'active'")
```

You can repartition the `DataFrame` using `DataFrame.repartition()` if needed. 
Once you have the `DataFrame`, you can register it as a table and use Spark 
SQL to query it:
```
dataFrame.registerTempTable("customer")
val SQLResult = sqlContext.sql("SELECT * FROM customer WHERE id > 100")
```

##Serialization
If the OQL query involves User Defined Type (UDT), and the default Java 
serializer is used, then the UDT on GemFire must implement `java.io.Serializable`.

If KryoSerializer is preferred, as described in [Spark Documentation]
(https://spark.apache.org/docs/latest/tuning.html), you can configure 
`SparkConf` as the following example:
```
val conf = new SparkConf()
  .setAppName("MySparkApp")
  .setMaster("local[*]")
  .set(GemFireLocatorPropKey, "localhost[55221]")
  .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  .set("spark.kryo.registrator", "io.pivotal.gemfire.spark.connector.GemFireKryoRegistrator")
```

and register the classes (optional)
```
conf.registerKryoClasses(Array(classOf[MyClass1], classOf[MyClass2]))
```

Use the following options to start Spark shell:
```
 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer
 --conf spark.kryo.registrator=io.pivotal.gemfire.spark.connector.GemFireKryoRegistrator
```

## References
[GemFire OQL Documentation](http://gemfire.docs.pivotal.io/latest/userguide/index.html#developing/querying_basics/chapter_overview.html)

[GemFire OQL Examples](http://gemfire.docs.pivotal.io/latest/userguide/index.html#getting_started/quickstart_examples/querying.html)

[Spark SQL Documentation](https://spark.apache.org/docs/latest/sql-programming-guide.html)


Next: [Using Connector in Java](9_java_api.md)
