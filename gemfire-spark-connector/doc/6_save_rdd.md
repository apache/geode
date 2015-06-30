## Saving RDD to GemFire

It is possible to save any RDD to a GemFire region. The requirements are:
 - the object class of the elements contained by the RDD is 
   (1) available on the classpath of GemFire servers 
   (2) and serializable.
 - the target region exists.

To save an RDD to an existing GemFire region, import 
`io.pivotal.gemfire.spark.connector._` and call the `saveToGemfire` 
method on RDD.

### Save RDD[(K, V)] to GemFire
For pair RDD, i.e., RDD[(K, V)], the pair is treated as key/value pair.
```
val data = Array(("1","one"),("2","two"),("3","three"))
val rdd = sc.parallelize(data)
rdd.saveToGemfire("str_str_region")
```

If you create GemFireConnectionConf as described in 
[Connecting to Gemfire](3_connecting.md), the last statement becomes:
```
rdd.saveToGemFire("str_str_region", connConf)
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

Note that GemFire regions require unique keys, so if the pair RDD 
contains duplicated keys, those pairs with the same key are overwriting
each other, and only one of them appears in the final dataset.

### Save RDD[T] to GemFire
To save non-pair RDD to GemFire, a function (`f: T => K`) that creates keys
from elements of RDD, and is used to convert RDD element `T` to pair `(f(T), T)`,
then the pair is save to GemFire.

```
val data2 = Array("a","ab","abc")
val rdd2 = sc.parallelize(data2)
rdd2.saveToGemfire("str_int_region", e => (e, e.length))
// or use GemFireConnectionConf object directly
// rdd2.saveToGemfire("rgnb", e => (e, e.length), connConf)
```

 
Next: [Saving DStream to GemFire](7_save_dstream.md)
