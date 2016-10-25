## Loading Data from Geode

To expose full data set of a Geode region as a Spark
RDD, call `geodeRegion` method on the SparkContext object.

```
val rdd = sc.geodeRegion("region path")
```

Or with specific `GeodeConectionConf` object instance (see 
[Connecting to  Geode](3_connecting.md) for how to create GeodeConectionConf):
```
val rdd = sc.geodeRegion("region path", connConf)
```

## Geode RDD Partitions

Geode has two region types: **replicated**, and
**partitioned** region. Replicated region has full dataset on
each server, while partitioned region has its dataset spanning
upon multiple servers, and may have duplicates for high 
availability.

Since replicated region has its full dataset available on every
server, there is only one RDD partition for a `GeodeRegionRDD` that 
represents a replicated region.

For a `GeodeRegionRDD` that represents a partitioned region, there are 
many potential  ways to create RDD partitions. So far, we have 
implemented ServerSplitsPartitioner, which will split the bucket set
on each Geode server into two RDD partitions by default.
The number of splits is configurable, the following shows how to set 
three partitions per Geode server:
```
import org.apache.geode.spark.connector._

val opConf = Map(PreferredPartitionerPropKey -> ServerSplitsPartitionerName,
                 NumberPartitionsPerServerPropKey -> "3")

val rdd1 = sc.geodeRegion[String, Int]("str_int_region", opConf = opConf)
// or
val rdd2 = sc.geodeRegion[String, Int]("str_int_region", connConf, opConf)  
```


## Geode Server-Side Filtering
Server-side filtering allow exposing partial dataset of a Geode region
as a RDD, this reduces the amount of data transferred from Geode to 
Spark to speed up processing.
```
val rdd = sc.geodeRegion("<region path>").where("<where clause>")
```

The above call is translated to OQL query `select key, value from /<region path>.entries where <where clause>`, then 
the query is executed for each RDD partition. Note: the RDD partitions are created the same way as described in the 
section above.

In the following demo, javabean class `Emp` is used, it has 5 attributes: `id`, `lname`, `fname`, `age`, and `loc`. 
In order to make `Emp` class available on Geode servers, we need to deploy a jar file that contains `Emp` class, 
now build the `emp.jar`,  deploy it and create region `emps` in `gfsh`:
```
zip $CONNECTOR/geode-spark-demos/basic-demos/target/scala-2.10/basic-demos_2.10-0.5.0.jar \
  -i "demo/Emp.class" --out $CONNECTOR/emp.jar
  
gfsh
gfsh> deploy --jar=<path to connector project>/emp.jar
gfsh> create region --name=emps --type=PARTITION 
```
Note: The `Emp.class` is availble in `basic-demos_2.10-0.5.0.jar`. But that jar file depends on many scala and spark 
classes that are not available on Geode servers' classpath. So use the above `zip` command to create a jar file that 
only contains `Emp.class`.  

Now in Spark shell, generate some random `Emp` records, and save them to region `emps` (remember to add `emp.jar` to 
Spark shell classpath before starting Spark shell):
```
import org.apache.geode.spark.connector._
import scala.util.Random
import demo.Emp

val lnames = List("Smith", "Johnson", "Jones", "Miller", "Wilson", "Taylor", "Thomas", "Lee", "Green", "Parker", "Powell")
val fnames = List("John", "James", "Robert", "Paul", "George", "Kevin", "Jason", "Jerry", "Peter", "Joe", "Alice", "Sophia", "Emma", "Emily")
val locs = List("CA", "WA", "OR", "NY", "FL")
def rpick(xs: List[String]): String = xs(Random.nextInt(xs.size))

val d1 = (1 to 20).map(x => new Emp(x, rpick(lnames), rpick(fnames), 20+Random.nextInt(41), rpick(locs))).toArray
val rdd1 = sc.parallelize(d1) 
rdd1.saveToGeode("emps", e => (e.getId, e))
```

Now create a RDD that contains all employees whose age is less than 40, and display its contents:
```
val rdd1s = sc.geodeRegion("emps").where("value.getAge() < 40")

rdd1s.foreach(println)
(5,Emp(5, Taylor, Robert, 32, FL))
(14,Emp(14, Smith, Jason, 28, FL))
(7,Emp(7, Jones, Robert, 26, WA))
(17,Emp(17, Parker, John, 20, WA))
(2,Emp(2, Thomas, Emily, 22, WA))
(10,Emp(10, Lee, Alice, 31, OR))
(4,Emp(4, Wilson, James, 37, CA))
(15,Emp(15, Powell, Jason, 34, NY))
(3,Emp(3, Lee, Sophia, 32, OR))
(9,Emp(9, Johnson, Sophia, 25, OR))
(6,Emp(6, Miller, Jerry, 30, NY))
```

Next: [RDD Join and Outer Join Geode Region](5_rdd_join.md)
