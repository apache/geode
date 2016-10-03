## Saving RDD to Geode

It is possible to save any RDD to a Geode region. The requirements are:
 - the object class of the elements contained by the RDD is 
   (1) available on the classpath of Geode servers 
   (2) and serializable.
 - the target region exists.

To save an RDD to an existing Geode region, import 
`org.apache.geode.spark.connector._` and call the `saveToGeode` 
method on RDD.

### Save RDD[(K, V)] to Geode
For pair RDD, i.e., RDD[(K, V)], the pair is treated as key/value pair.
```
val data = Array(("1","one"),("2","two"),("3","three"))
val rdd = sc.parallelize(data)
rdd.saveToGeode("str_str_region")
```

If you create GeodeConnectionConf as described in 
[Connecting to Geode](3_connecting.md), the last statement becomes:
```
rdd.saveToGeode("str_str_region", connConf)
```

You can verify the region contents:
```
gfsh>query --query="select key, value from /str_str_region.entrySet"

Result     : true
startCount : 0
endCount   : 20
Rows       : 3

key | value
--- | -----
1   | one
3   | three
2   | two
```

Note that Geode regions require unique keys, so if the pair RDD 
contains duplicated keys, those pairs with the same key are overwriting
each other, and only one of them appears in the final dataset.

### Save RDD[T] to Geode
To save non-pair RDD to Geode, a function (`f: T => K`) that creates keys
from elements of RDD, and is used to convert RDD element `T` to pair `(f(T), T)`,
then the pair is save to Geode.

```
val data2 = Array("a","ab","abc")
val rdd2 = sc.parallelize(data2)
rdd2.saveToGeode("str_int_region", e => (e, e.length))
// or use GeodeConnectionConf object directly
// rdd2.saveToGeode("rgnb", e => (e, e.length), connConf)
```

### `rdd.save.batch.size` 

The connector invokes Geode API `putAll()` to save the data. To make
`putAll()` more efficient, the connector invokes putAll() for every 
10,000 entries by default. This batch size can be changed with optional
parameter `opConf`. The following shows how to do it:

```
  // in Scala
  rdd.saveToGeode(regionPath, opConf = Map(RDDSaveBatchSizePropKey -> "5000"))

  // in Java
  Properties opConf = new Properties();
  opConf.put(RDDSaveBatchSizePropKey, "5000");
  ...
  javaFunctions(rdd).saveToGeode(regionPath, opConf); 
   
  // note: RDDSaveBatchSizePropKey = "rdd.save.batch.size" 
```


Next: [Saving DStream to Geode](7_save_dstream.md)
