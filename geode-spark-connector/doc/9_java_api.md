## Using Connector in Java
This section describes how to access the functionality of Spark Geode 
Connector when you write your Spark applications in Java. It is assumed
that you already familiarized yourself with the previous sections and 
understand how the Spark Geode Connector works.

### Prerequisites
The best way to use the Spark Geode Connector Java API is to statically
import all of the methods in `GeodeJavaUtil`. This utility class is
the main entry point for Spark Geode Connector Java API.
```
import static org.apache.geode.spark.connector.javaapi.GeodeJavaUtil.*;
```

Create JavaSparkContext (don't forget about the static import):
```
SparkConf conf = new SparkConf();
conf.set(GeodeLocatorPropKey, "192.168.1.47[10334]")
JavaSparkContext jsc = new JavaSparkContext(conf);
```

### Accessing Geode region in Java
Geode region is exposed as `GeodeJavaRegionRDD<K,V>`(subclass of
`JavaPairRDD<K, V>`):
```
GeodeJavaRegionRDD<Int, Emp> rdd1 = javaFunctions(jsc).geodeRegion("emps")
GeodeJavaRegionRDD<Int, Emp> rdd2 = rdd1.where("value.getAge() < 40");
```

### RDD Join and Outer Join
Use the `rdd3` and region `emps` from [join and outer join examples](5_rdd_join.md):
```
static class MyKeyFunction implements Function<Tuple2<String, Integer>, Integer> {
  @Override public Interger call(Tuple2<String, Integer> pair) throws Exception {
    return pair._2();
  }
}

MyKeyFunction func = new MyKeyFunction();

JavaPairRDD<Tuple2<String, Integer>, Emp> rdd3j =
  javaFunction(rdd3).joinGeodeRegion("emps", func);

JavaPairRDD<Tuple2<String, Integer>, Option<Emp>> rdd3o = 
  javaFunction(rdd3).outerJoinGeodeRegion("emps", func);

```

### Saving JavaPairRDD to Geode
Saving JavaPairRDD is straightforward:
```
List<Tuple2<String, String>> data = new ArrayList<>();
data.add(new Tuple2<>("7", "seven"));
data.add(new Tuple2<>("8", "eight"));
data.add(new Tuple2<>("9", "nine"));

// create JavaPairRDD
JavaPairRDD<String, String> rdd1 = jsc.parallelizePairs(data);
// save to Geode
javaFunctions(rdd1).saveToGeode("str_str_region");
```

In order to save `JavaRDD<Tuple2<K,V>>`, it needs to be converted to 
`JavaPairRDD<K,V>` via static method `toJavaPairRDD` from `GeodeJavaUtil`:
```
List<Tuple2<String, String>> data2 = new ArrayList<Tuple2<String, String>>();
data2.add(new Tuple2<>("11", "eleven"));
data2.add(new Tuple2<>("12", "twelve"));
data2.add(new Tuple2<>("13", "thirteen"));

// create JavaRDD<Tuple2<K,V>>
JavaRDD<Tuple2<String, String>> rdd2 =  jsc.parallelize(data2);
// save to Geode
javaFunctions(toJavaPairRDD(rdd2)).saveToGeode("str_str_region");
``` 

### Saving JavaRDD to Geode
Similar to Scala version, a function is required to generate key/value pair
from RDD element. The following `PairFunction` generate a `<String, Integer>`
pair from `<String>`:
```
PairFunction<String, String, Integer> pairFunc =  
  new PairFunction<String, String, Integer>() {
    @Override public Tuple2<String, Integer> call(String s) throws Exception {
      return new Tuple2<String, Integer>(s, s.length());
    }
  };
```
Note: there are 3 type parameters for PairFunction, they are: 
 1. type of JavaRDD element
 2. type of key of output key/value pair
 3. type of value of output key/value pair

Once `PairFunction` is ready, the rest is easy:
```
// create demo JavaRDD<String>
List<String> data = new ArrayList<String>();
data.add("a");
data.add("ab");
data.add("abc");
JavaRDD<String> jrdd =  sc.parallelize(data);
    
javaFunctions(rdd).saveToGeode("str_int_region", pairFunc);
```

### Saving JavaPairDStream and JavaDStream
Saving JavaPairDStream and JavaDStream is similar to saving JavaPairRDD 
jand JavaRDD:
```
JavaPairDStream<String, String> ds1 = ...
javaFunctions(ds1).saveToGeode("str_str_region");

JavaDStream<String> ds2 = ...
javaFunctions(ds2).saveToGeode("str_int_region", pairFunc);
```

### Using Geode OQL

There are two geodeOQL Java APIs, with and without GeodeConnectionConf.
Here is an example without GeodeConnectionConf, it will use default 
GeodeConnectionConf internally.
```
// assume there's jsc: JavaSparkContext
SQLContext sqlContext = new org.apache.spark.sql.SQLContext(jsc);
DataFrame df = javaFunctions(sqlContext).geodeOQL("select * from /str_str_region");
df.show();
```

Next: [About The Demos] (10_demos.md)
