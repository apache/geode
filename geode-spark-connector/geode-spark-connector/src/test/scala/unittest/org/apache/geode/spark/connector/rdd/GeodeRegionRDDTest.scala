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

import org.apache.geode.cache.Region
import org.apache.geode.spark.connector.internal.RegionMetadata
import org.apache.geode.spark.connector.internal.rdd.{GeodeRDDPartition, GeodeRegionRDD}
import org.apache.geode.spark.connector.{GeodeConnectionConf, GeodeConnection}
import org.apache.spark.{TaskContext, Partition, SparkContext}
import org.mockito.Mockito._
import org.mockito.Matchers.{eq => mockEq, any => mockAny}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FunSuite}

import scala.reflect.ClassTag

class GeodeRegionRDDTest extends FunSuite with Matchers with MockitoSugar {

  /** create common mocks, not all mocks are used by all tests */
  def createMocks[K, V](regionPath: String)(implicit kt: ClassTag[K], vt: ClassTag[V], m: Manifest[Region[K, V]])
    : (String, Region[K,V], GeodeConnectionConf, GeodeConnection) = {
    val mockConnection = mock[GeodeConnection]
    val mockRegion = mock[Region[K, V]]
    val mockConnConf = mock[GeodeConnectionConf]
    when(mockConnConf.getConnection).thenReturn(mockConnection)
    when(mockConnection.getRegionProxy[K, V](regionPath)).thenReturn(mockRegion)
    when(mockConnConf.locators).thenReturn(Seq.empty)
    (regionPath, mockRegion, mockConnConf, mockConnection)
  }
  
  test("create GeodeRDD with non-existing region") {
    val (regionPath, mockRegion, mockConnConf, mockConnection) = createMocks[String, String]("test")
    when(mockConnConf.getConnection).thenReturn(mockConnection)
    when(mockConnection.validateRegion[String,String](regionPath)).thenThrow(new RuntimeException)
    val mockSparkContext = mock[SparkContext]
    intercept[RuntimeException] { GeodeRegionRDD[String, String](mockSparkContext, regionPath, mockConnConf) }
    verify(mockConnConf).getConnection
    verify(mockConnection).validateRegion[String, String](regionPath)
  }
  
  test("getPartitions with non-existing region") {
    // region exists when RDD is created, but get removed before getPartitions() is invoked
    val (regionPath, mockRegion, mockConnConf, mockConnection) = createMocks[String, String]("test")
    when(mockConnection.getRegionMetadata[String, String](regionPath)).thenReturn(None)
    val mockSparkContext = mock[SparkContext]
    intercept[RuntimeException] { GeodeRegionRDD[String, String](mockSparkContext, regionPath, mockConnConf).getPartitions }
  }

  test("getPartitions with replicated region and not preferred env") {
    val (regionPath, mockRegion, mockConnConf, mockConnection) = createMocks[String, String]("test")
    implicit val mockConnConf2 = mockConnConf
    val mockSparkContext = mock[SparkContext]
    when(mockConnection.getRegionMetadata[String, String](regionPath)).thenReturn(Some(new RegionMetadata(regionPath, false, 0, null)))
    val partitions = GeodeRegionRDD(mockSparkContext, regionPath, mockConnConf).partitions
    verifySinglePartition(partitions)
  }

  def verifySinglePartition(partitions: Array[Partition]): Unit = {
    assert(1 == partitions.size)
    assert(partitions(0).index === 0)
    assert(partitions(0).isInstanceOf[GeodeRDDPartition])
    assert(partitions(0).asInstanceOf[GeodeRDDPartition].bucketSet.isEmpty)
  }

  test("getPartitions with replicated region and preferred OnePartitionPartitioner") {
    // since it's replicated region, so OnePartitionPartitioner will be used, i.e., override preferred partitioner
    import org.apache.geode.spark.connector.{PreferredPartitionerPropKey, OnePartitionPartitionerName}
    val (regionPath, mockRegion, mockConnConf, mockConnection) = createMocks[String, String]("test")
    when(mockConnection.getRegionMetadata[String, String](regionPath)).thenReturn(Some(new RegionMetadata(regionPath, false, 0, null)))
    implicit val mockConnConf2 = mockConnConf
    val mockSparkContext = mock[SparkContext]
    val env = Map(PreferredPartitionerPropKey -> OnePartitionPartitionerName)
    val partitions = GeodeRegionRDD(mockSparkContext, regionPath, mockConnConf, env).partitions
    verifySinglePartition(partitions)
  }

  test("getPartitions with partitioned region and not preferred env") {
    val (regionPath, mockRegion, mockConnConf, mockConnection) = createMocks[String, String]("test")
    implicit val mockConnConf2 = mockConnConf
    val mockSparkContext = mock[SparkContext]
    when(mockConnection.getRegionMetadata[String, String](regionPath)).thenReturn(Some(new RegionMetadata(regionPath, true, 2, null)))
    val partitions = GeodeRegionRDD(mockSparkContext, regionPath, mockConnConf).partitions
    verifySinglePartition(partitions)
  }

  test("GeodeRDD.compute() method") {
    val (regionPath, mockRegion, mockConnConf, mockConnection) = createMocks[String, String]("test")
    implicit val mockConnConf2 = mockConnConf
    val mockIter = mock[Iterator[(String, String)]]
    val partition = GeodeRDDPartition(0, Set.empty)
    when(mockConnection.getRegionMetadata[String, String](regionPath)).thenReturn(Some(new RegionMetadata(regionPath, true, 2, null)))
    when(mockConnection.getRegionData[String, String](regionPath, None, partition)).thenReturn(mockIter)
    val mockSparkContext = mock[SparkContext]
    val rdd = GeodeRegionRDD[String, String](mockSparkContext, regionPath, mockConnConf)
    val partitions = rdd.partitions
    assert(1 == partitions.size)
    val mockTaskContext = mock[TaskContext]
    rdd.compute(partitions(0), mockTaskContext)        
    verify(mockConnection).getRegionData[String, String](mockEq(regionPath), mockEq(None), mockEq(partition))
    // verify(mockConnection).getRegionData[String, String](regionPath, Set.empty.asInstanceOf[Set[Int]], "geodeRDD 0.0")
  }

}
