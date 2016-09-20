/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package unittest.org.apache.geode.spark.connector.rdd

import org.apache.geode.distributed.internal.ServerLocation
import org.apache.geode.spark.connector.internal.RegionMetadata
import org.apache.geode.spark.connector.internal.rdd.GeodeRDDPartitioner._
import org.apache.geode.spark.connector.GeodeConnection
import org.apache.geode.spark.connector.internal.rdd._
import org.apache.spark.Partition
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FunSuite}

import java.util.{HashSet => JHashSet, HashMap => JHashMap}

import scala.collection.mutable

class GeodeRDDPartitionerTest extends FunSuite with Matchers with MockitoSugar {

  val emptyServerBucketMap: JHashMap[ServerLocation, JHashSet[Integer]] = new JHashMap()

  def toJavaServerBucketMap(map: Map[(String, Int), Set[Int]]): JHashMap[ServerLocation, JHashSet[Integer]] = {
    import scala.collection.JavaConversions._
    val tmp = map.map {case ((host, port), set) => (new ServerLocation(host, port), set.map(Integer.valueOf))}
    (new JHashMap[ServerLocation, JHashSet[Integer]]() /: tmp) { case (acc, (s, jset)) => acc.put(s, new JHashSet(jset)); acc }
  }
  
  val map: mutable.Map[(String, Int), mutable.Set[Int]] = mutable.Map(
    ("s0",1) -> mutable.Set.empty, ("s1",2) -> mutable.Set(0), ("s2",3) -> mutable.Set(1, 2), ("s3",4) -> mutable.Set(3, 4, 5))

  
  // update this test whenever change default setting 
  test("default partitioned region partitioner") {
    assert(GeodeRDDPartitioner.defaultPartitionedRegionPartitioner === ServerSplitsPartitioner)
  }

  // update this test whenever change default setting 
  test("default replicated region partitioner") {
    assert(GeodeRDDPartitioner.defaultReplicatedRegionPartitioner === OnePartitionPartitioner)
  }
  
  test("GeodeRDDPartitioner.apply method") {
    import org.apache.geode.spark.connector.internal.rdd.GeodeRDDPartitioner._
    for ((name, partitioner) <- partitioners) assert(GeodeRDDPartitioner(name) == partitioner)
    assert(GeodeRDDPartitioner("dummy") == GeodeRDDPartitioner.defaultPartitionedRegionPartitioner)
    assert(GeodeRDDPartitioner() == GeodeRDDPartitioner.defaultPartitionedRegionPartitioner)
  }
  
  test("OnePartitionPartitioner") {
    val mockConnection = mock[GeodeConnection]
    val partitions = OnePartitionPartitioner.partitions[String, String](mockConnection, null, Map.empty)
    verifySinglePartition(partitions)
  }

  def verifySinglePartition(partitions: Array[Partition]): Unit = {
    assert(1 == partitions.size)
    assert(partitions(0).index === 0)
    assert(partitions(0).isInstanceOf[GeodeRDDPartition])
    assert(partitions(0).asInstanceOf[GeodeRDDPartition].bucketSet.isEmpty)
  }

  test("ServerSplitsPartitioner.doPartitions(): n=1 & no empty bucket") {
    val map: List[(String, mutable.Set[Int])] = List(
      "server1" -> mutable.Set(3,2,1,0), "server2" -> mutable.Set(5,4))
    val partitions = ServerSplitsPartitioner.doPartitions(map, 6, 1)
    verifyPartitions(partitions, List(
      (Set(0, 1, 2, 3), Seq("server1")), (Set(4, 5), Seq("server2"))))
  }

  test("ServerSplitsPartitioner.doPartitions(): n=1 & 1 empty bucket") {
    val map: List[(String, mutable.Set[Int])] = List(
      "server1" -> mutable.Set(3,2,1,0), "server2" -> mutable.Set(5,4))
    val partitions = ServerSplitsPartitioner.doPartitions(map, 7, 1)
    verifyPartitions(partitions, List(
      (Set(0, 1, 2, 3, 6), Seq("server1")), (Set(4, 5), Seq("server2"))))
  }

  test("ServerSplitsPartitioner.doPartitions(): n=1 & 2 empty bucket") {
    val map: List[(String, mutable.Set[Int])] = List(
      "server1" -> mutable.Set(3,2,1,0), "server2" -> mutable.Set(5,4))
    val partitions = ServerSplitsPartitioner.doPartitions(map, 8, 1)
    verifyPartitions(partitions, List(
      (Set(0, 1, 2, 3, 6), Seq("server1")), (Set(4, 5, 7), Seq("server2"))))
  }

  test("ServerSplitsPartitioner.doPartitions(): n=1 & 5 empty bucket") {
    val map: List[(String, mutable.Set[Int])] = List(
      "server1" -> mutable.Set(3,2,1,0), "server2" -> mutable.Set(5,4))
    val partitions = ServerSplitsPartitioner.doPartitions(map, 11, 1)
    verifyPartitions(partitions, List(
      (Set(0, 1, 2, 3, 6, 7, 8), Seq("server1")), (Set(4, 5, 9, 10), Seq("server2"))))
  }

  test("ServerSplitsPartitioner.doPartitions(): n=1, 4 empty-bucket, non-continuous IDs") {
    val map: List[(String, mutable.Set[Int])] = List(
      "server1" -> mutable.Set(1, 3), "server2" -> mutable.Set(5,7))
    val partitions = ServerSplitsPartitioner.doPartitions(map, 8, 1)
    verifyPartitions(partitions, List(
      (Set(0, 1, 2, 3), Seq("server1")), (Set(4, 5, 6, 7), Seq("server2"))))
  }

  test("ServerSplitsPartitioner.doPartitions(): n=2, no empty buckets, 3 servers have 1, 2, and 3 buckets") {
    val map: List[(String, mutable.Set[Int])] = List(
      "s1" -> mutable.Set(0), "s2" -> mutable.Set(1, 2), "s3" -> mutable.Set(3, 4, 5))
    val partitions = ServerSplitsPartitioner.doPartitions(map, 6, 2)
    // partitions.foreach(println)
    verifyPartitions(partitions, List(
      (Set(0), Seq("s1")), (Set(1), Seq("s2")), (Set(2), Seq("s2")), (Set(3, 4), Seq("s3")), (Set(5), Seq("s3"))))
  }

  test("ServerSplitsPartitioner.doPartitions(): n=3, no empty buckets, 4 servers have 0, 2, 3, and 4 buckets") {
    val map: List[(String, mutable.Set[Int])] = List(
      "s0" -> mutable.Set.empty, "s1" -> mutable.Set(0, 1), "s2" -> mutable.Set(2, 3, 4), "s3" -> mutable.Set(5, 6, 7, 8))
    val partitions = ServerSplitsPartitioner.doPartitions(map, 9, 3)
    // partitions.foreach(println)
    verifyPartitions(partitions, List(
      (Set(0), Seq("s1")), (Set(1), Seq("s1")), (Set(2), Seq("s2")), (Set(3), Seq("s2")), (Set(4), Seq("s2")),
      (Set(5, 6), Seq("s3")), (Set(7, 8), Seq("s3")) ))
  }

  test("ServerSplitsPartitioner.partitions(): metadata = None ") {
    val regionPath = "test"
    val mockConnection = mock[GeodeConnection]
    intercept[RuntimeException] { ServerSplitsPartitioner.partitions[String, String](mockConnection, null, Map.empty) }
  }

  test("ServerSplitsPartitioner.partitions(): replicated region ") {
    val regionPath = "test"
    val mockConnection = mock[GeodeConnection]
    val md = new RegionMetadata(regionPath, false, 11, null)
    when(mockConnection.getRegionMetadata(regionPath)).thenReturn(Some(md))
    val partitions = ServerSplitsPartitioner.partitions[String, String](mockConnection, md, Map.empty)
    verifySinglePartition(partitions)
  }

  test("ServerSplitsPartitioner.partitions(): partitioned region w/o data ") {
    val regionPath = "test"
    val mockConnection = mock[GeodeConnection]
    val md = new RegionMetadata(regionPath, true, 6, emptyServerBucketMap)
    when(mockConnection.getRegionMetadata(regionPath)).thenReturn(Some(md))
    val partitions = ServerSplitsPartitioner.partitions[String, String](mockConnection, md, Map.empty)
    verifySinglePartition(partitions)
  }

  test("ServerSplitsPartitioner.partitions(): partitioned region w/ some data ") {
    import org.apache.geode.spark.connector.NumberPartitionsPerServerPropKey
    val regionPath = "test"
    val mockConnection = mock[GeodeConnection]
    val map: Map[(String, Int), Set[Int]] = Map(
      ("s0",1) -> Set.empty, ("s1",2) -> Set(0), ("s2",3) -> Set(1, 2), ("s3",4) -> Set(3, 4, 5))
    val md = new RegionMetadata(regionPath, true, 6, toJavaServerBucketMap(map))
    when(mockConnection.getRegionMetadata(regionPath)).thenReturn(Some(md))
    val partitions = ServerSplitsPartitioner.partitions[String, String](mockConnection, md, Map(NumberPartitionsPerServerPropKey->"2"))
    // partitions.foreach(println)
    verifyPartitions(partitions, List(
      (Set(0), Seq("s1")), (Set(1), Seq("s2")), (Set(2), Seq("s2")), (Set(3, 4), Seq("s3")), (Set(5), Seq("s3"))))
  }
  
  // Note: since the order of partitions is not pre-determined, we have to verify partition id
  // and contents separately
  def verifyPartitions(partitions: Array[Partition], expPartitions: List[(Set[Int], Seq[String])]): Unit = {
    // 1. check size
    assert(partitions.size == expPartitions.size)
    // 2. check IDs are 0 to n-1
    (0 until partitions.size).toList.zip(partitions).foreach { case (id, p) => assert(id == p.index) }

    // 3. get all pairs of bucket set and its locations, and compare to the expected pairs
    val list = partitions.map { e =>
      val p = e.asInstanceOf[GeodeRDDPartition]
      (p.bucketSet, p.locations)
    }
    expPartitions.foreach(e => assert(list.contains(e)))    
  }

}
