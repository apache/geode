## RDD Join and Outer Join Geode Region

The Spark Geode Connector suports using any RDD as a source
of a join and outer join with a Geode region through APIs
`joinGeodeRegion[K, V]` and `outerJoinGeodeRegion[K, V]`. 
Those two APIs execute a single `region.getAll` call for every 
partition of the source RDD, so no unnecessary data will be requested
or transferred. This means a join or outer join between any RDD and
a Geode region can be performed without full region scan, and the
source RDD's partitioning and placement for data locality are used.

Please note that the two type parameters `[K, V]` are the type
of key/value pair of region entries, they need to be specified
to make result RDD has correct type.

The region `emps` that is created and populated in 
[Geode Server-Side Filtering](4_loading.md) will be used in the
following examples.

### RDD[(K, V1)] join and outer join Region[K, V2]

In this case, the source RDD is a pair RDD,  and it has the same key
type as the Region. Use API `rdd.joinGeodeRegion[K, V2](regionPath)` and 
`rdd.outerJoinGeodeRegion[K, V2](regionPath)` do the join and outer
join. 

Prepare a source RDD `rdd2`:
```
val d2 = (11 to 25).map(x => (x, s"message-$x")).toArray
val rdd2 = sc.parallelize(d2)
// print rdd2's content
rdd2.foreach(println)
(11,message-11)
(12,message-12)
(13,message-13)
(14,message-14)
(15,message-15)
(16,message-16)
(17,message-17)
(18,message-18)
(19,message-19)
(20,message-20)
(21,message-21)
(22,message-22)
(23,message-23)
(24,message-24)
(25,message-25)
```

Join RDD `rdd2` with region `emps`, and print out the result:
```
val rdd2j = rdd2.joinGeodeRegion[Int, Emp]("emps")

rdd2j.foreach(println)
((11,message-11),Emp(11, Taylor, Emma, 44, CA))
((12,message-12),Emp(12, Taylor, Joe, 60, FL))
((13,message-13),Emp(13, Lee, Kevin, 50, FL))
((14,message-14),Emp(14, Smith, Jason, 28, FL))
((15,message-15),Emp(15, Powell, Jason, 34, NY))
((16,message-16),Emp(16, Thomas, Alice, 42, OR))
((17,message-17),Emp(17, Parker, John, 20, WA))
((18,message-18),Emp(18, Powell, Alice, 58, FL))
((19,message-19),Emp(19, Taylor, Peter, 46, FL))
((20,message-20),Emp(20, Green, Peter, 57, CA))
```
Note that there's no pairs in the result RDD `rdd2j` corresponding to
the pairs with id from 21 to 25 in RDD `rdd2` since there's no region
entries have those key values.

Outer join RDD `rdd2` with region `emps`, and print out the result:
```
val rdd2o = rdd2.outerJoinGeodeRegion[Int, Emp]("emps")

rdd2o.foreach(println)
((18,message-18),Some(Emp(18, Powell, Alice, 58, FL)))
((19,message-19),Some(Emp(19, Taylor, Peter, 46, FL)))
((11,message-11),Some(Emp(11, Taylor, Emma, 44, CA)))
((12,message-12),Some(Emp(12, Taylor, Joe, 60, FL)))
((20,message-20),Some(Emp(20, Green, Peter, 57, CA)))
((21,message-21),None)
((22,message-22),None)
((23,message-23),None)
((24,message-24),None)
((25,message-25),None)
((13,message-13),Some(Emp(13, Lee, Kevin, 50, FL)))
((14,message-14),Some(Emp(14, Smith, Jason, 28, FL)))
((15,message-15),Some(Emp(15, Powell, Jason, 34, NY)))
((16,message-16),Some(Emp(16, Thomas, Alice, 42, OR)))
((17,message-17),Some(Emp(17, Parker, John, 20, WA)))
```
Note that there are pairs in the result RDD `rdd2o` corresponding to
the pairs with id from 21 to 25 in the RDD `rdd2`, and values are `None`
since there's no region entries have those key values.

### RDD[(K1, V1)] join and outer join Region[K2, V2]

In this case, the source RDD is still a pair RDD,  but it has different
key type. Use API `rdd.joinGeodeRegion[K2, V2](regionPath, func)` and 
`rdd.outerJoinGeodeRegion[K2, V2](regionPath, func)` do the join and 
outer join, where `func` is the function to generate key from (k, v)
pair, the element of source RDD, to join with Geode region.

Prepare a source RDD `d3`:
```
val d3 = (11 to 25).map(x => (s"message-$x", x)).toArray
val rdd3 = sc.parallelize(d3)
// print rdd3's content
rdd3.foreach(println)
(message-18,18)
(message-19,19)
(message-11,11)
(message-20,20)
(message-21,21)
(message-22,22)
(message-12,12)
(message-23,23)
(message-24,24)
(message-25,25)
(message-13,13)
(message-14,14)
(message-15,15)
(message-16,16)
(message-17,17)
```

Join RDD `rdd3` (RDD[(String, Int)] with region `emps` (Region[Int, Emp]), and print out the result:
```
val rdd3j = rdd3.joinGeodeRegion[Int, Emp]("emps", pair => pair._2)

rdd3j.foreach(println)
((message-18,18),Emp(18, Powell, Alice, 58, FL))
((message-19,19),Emp(19, Taylor, Peter, 46, FL))
((message-20,20),Emp(20, Green, Peter, 57, CA))
((message-11,11),Emp(11, Taylor, Emma, 44, CA))
((message-12,12),Emp(12, Taylor, Joe, 60, FL))
((message-13,13),Emp(13, Lee, Kevin, 50, FL))
((message-14,14),Emp(14, Smith, Jason, 28, FL))
((message-15,15),Emp(15, Powell, Jason, 34, NY))
((message-16,16),Emp(16, Thomas, Alice, 42, OR))
((message-17,17),Emp(17, Parker, John, 20, WA))
```
Note `pair => pair._2` means use the 2nd element of the element of source
RDD and join key.

Outer join RDD `rdd3` with region `emps`, and print out the result:
```
val rdd3o = rdd3.outerJoinGeodeRegion[Int, Emp]("emps", pair => pair._2)

rdd3o.foreach(println)
((message-18,18),Some(Emp(18, Powell, Alice, 58, FL)))
((message-11,11),Some(Emp(11, Taylor, Emma, 44, CA)))
((message-19,19),Some(Emp(19, Taylor, Peter, 46, FL)))
((message-12,12),Some(Emp(12, Taylor, Joe, 60, FL)))
((message-20,20),Some(Emp(20, Green, Peter, 57, CA)))
((message-13,13),Some(Emp(13, Lee, Kevin, 50, FL)))
((message-21,21),None)
((message-14,14),Some(Emp(14, Smith, Jason, 28, FL)))
((message-22,22),None)
((message-23,23),None)
((message-24,24),None)
((message-25,25),None)
((message-15,15),Some(Emp(15, Powell, Jason, 34, NY)))
((message-16,16),Some(Emp(16, Thomas, Alice, 42, OR)))
((message-17,17),Some(Emp(17, Parker, John, 20, WA)))
```

### RDD[T] join and outer join Region[K, V]

Use API `rdd.joinGeodeRegion[K, V](regionPath, func)` and 
`rdd.outerJoinGeodeRegion[K, V](regionPath, func)` do the join
and outer join, where `func` is the function to generate key from
`t`, the element of source RDD, to join with Geode region.

Prepare a source RDD `d4`:
```
val d4 = (11 to 25).map(x => x * 2).toArray
val rdd4 = sc.parallelize(d4)
// print rdd4's content
rdd4.foreach(println)
22
24
36
38
40
42
44
46
26
28
48
30
32
34
50
```

Join RDD `d4` with region `emps`, and print out the result:
```
val rdd4j = rdd4.joinGeodeRegion[Int, Emp]("emps", x => x/2)

rdd4j.foreach(println)
(22,Emp(11, Taylor, Emma, 44, CA))
(24,Emp(12, Taylor, Joe, 60, FL))
(26,Emp(13, Lee, Kevin, 50, FL))
(28,Emp(14, Smith, Jason, 28, FL))
(30,Emp(15, Powell, Jason, 34, NY))
(32,Emp(16, Thomas, Alice, 42, OR))
(34,Emp(17, Parker, John, 20, WA))
(36,Emp(18, Powell, Alice, 58, FL))
(38,Emp(19, Taylor, Peter, 46, FL))
(40,Emp(20, Green, Peter, 57, CA))
```

Outer join RDD `d4` with region `emps`, and print out the result:
```
val rdd4o = rdd4.outerJoinGeodeRegion[Int, Emp]("emps", x => x/2)

rdd4o.foreach(println)
(36,Some(Emp(18, Powell, Alice, 58, FL)))
(38,Some(Emp(19, Taylor, Peter, 46, FL)))
(40,Some(Emp(20, Green, Peter, 57, CA)))
(42,None)
(44,None)
(46,None)
(48,None)
(50,None)
(22,Some(Emp(11, Taylor, Emma, 44, CA)))
(24,Some(Emp(12, Taylor, Joe, 60, FL)))
(26,Some(Emp(13, Lee, Kevin, 50, FL)))
(28,Some(Emp(14, Smith, Jason, 28, FL)))
(30,Some(Emp(15, Powell, Jason, 34, NY)))
(32,Some(Emp(16, Thomas, Alice, 42, OR)))
(34,Some(Emp(17, Parker, John, 20, WA)))
```


Next: [Saving RDD to Geode](6_save_rdd.md)
