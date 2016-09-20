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
package unittest.org.apache.geode.spark.connector

import org.apache.geode.cache.Region
import org.apache.geode.spark.connector._
import org.apache.geode.spark.connector.internal.rdd.{GeodeRDDWriter, GeodePairRDDWriter}
import org.apache.spark.{TaskContext, SparkContext}
import org.apache.spark.rdd.RDD
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSuite, Matchers}
import collection.JavaConversions._
import scala.reflect.ClassTag
import org.mockito.Matchers.{eq => mockEq, any => mockAny}

class GeodeRDDFunctionsTest extends FunSuite with Matchers with MockitoSugar {

  test("test PairRDDFunction Implicit") {
    import org.apache.geode.spark.connector._
    val mockRDD = mock[RDD[(Int, String)]]
    // the implicit make the following line valid
    val pairRDD: GeodePairRDDFunctions[Int, String] = mockRDD
    pairRDD shouldBe a [GeodePairRDDFunctions[_, _]]
  }
  
  test("test RDDFunction Implicit") {
    import org.apache.geode.spark.connector._
    val mockRDD = mock[RDD[String]]
    // the implicit make the following line valid
    val nonPairRDD: GeodeRDDFunctions[String] = mockRDD
    nonPairRDD shouldBe a [GeodeRDDFunctions[_]]
  }

  def createMocks[K, V](regionPath: String)
    (implicit kt: ClassTag[K], vt: ClassTag[V], m: Manifest[Region[K, V]]): (String, GeodeConnectionConf, GeodeConnection, Region[K, V]) = {
    val mockConnection = mock[GeodeConnection]
    val mockConnConf = mock[GeodeConnectionConf]
    val mockRegion = mock[Region[K, V]]
    when(mockConnConf.getConnection).thenReturn(mockConnection)
    when(mockConnection.getRegionProxy[K, V](regionPath)).thenReturn(mockRegion)
    // mockRegion shouldEqual mockConn.getRegionProxy[K, V](regionPath)
    (regionPath, mockConnConf, mockConnection, mockRegion)
  }

  test("test GeodePairRDDWriter") {
    val (regionPath, mockConnConf, mockConnection, mockRegion) = createMocks[String, String]("test")
    val writer = new GeodePairRDDWriter[String, String](regionPath, mockConnConf)
    val data = List(("1", "one"), ("2", "two"), ("3", "three"))
    writer.write(null, data.toIterator)
    val expectedMap: Map[String, String] = data.toMap
    verify(mockRegion).putAll(expectedMap)
  }

  test("test GeodeNonPairRDDWriter") {
    val (regionPath, mockConnConf, mockConnection, mockRegion) = createMocks[Int, String]("test")
    val writer = new GeodeRDDWriter[String, Int, String](regionPath, mockConnConf)
    val data = List("a", "ab", "abc")
    val f: String => (Int, String) = s => (s.length, s)
    writer.write(f)(null, data.toIterator)
    val expectedMap: Map[Int, String] = data.map(f).toMap
    verify(mockRegion).putAll(expectedMap)
  }
  
  test("test PairRDDFunctions.saveToGeode") {
    verifyPairRDDFunction(useOpConf = false)
  }

  test("test PairRDDFunctions.saveToGeode w/ opConf") {
    verifyPairRDDFunction(useOpConf = true)
  }
  
  def verifyPairRDDFunction(useOpConf: Boolean): Unit = {
    import org.apache.geode.spark.connector._
    val (regionPath, mockConnConf, mockConnection, mockRegion) = createMocks[String, String]("test")
    val mockRDD = mock[RDD[(String, String)]]
    val mockSparkContext = mock[SparkContext]
    when(mockRDD.sparkContext).thenReturn(mockSparkContext)
    val result = 
      if (useOpConf) 
        mockRDD.saveToGeode(regionPath, mockConnConf, Map(RDDSaveBatchSizePropKey -> "5000"))
      else
        mockRDD.saveToGeode(regionPath, mockConnConf)
    verify(mockConnection, times(1)).validateRegion[String, String](regionPath)
    result === Unit
    verify(mockSparkContext, times(1)).runJob[(String, String), Unit](
      mockEq(mockRDD), mockAny[(TaskContext, Iterator[(String, String)]) => Unit])(mockAny(classOf[ClassTag[Unit]]))

    // Note: current implementation make following code not compilable
    //       so not negative test for this case
    //  val rdd: RDD[(K, V)] = ...
    //  rdd.saveToGeode(regionPath, s => (s.length, s))
  }

  test("test RDDFunctions.saveToGeode") {
    verifyRDDFunction(useOpConf = false)
  }

  test("test RDDFunctions.saveToGeode w/ opConf") {
    verifyRDDFunction(useOpConf = true)
  }
  
  def verifyRDDFunction(useOpConf: Boolean): Unit = {
    import org.apache.geode.spark.connector._
    val (regionPath, mockConnConf, mockConnection, mockRegion) = createMocks[Int, String]("test")
    val mockRDD = mock[RDD[(String)]]
    val mockSparkContext = mock[SparkContext]
    when(mockRDD.sparkContext).thenReturn(mockSparkContext)
    val result = 
      if (useOpConf)
        mockRDD.saveToGeode(regionPath, s => (s.length, s), mockConnConf, Map(RDDSaveBatchSizePropKey -> "5000"))
      else
        mockRDD.saveToGeode(regionPath, s => (s.length, s), mockConnConf)
    verify(mockConnection, times(1)).validateRegion[Int, String](regionPath)
    result === Unit
    verify(mockSparkContext, times(1)).runJob[String, Unit](
      mockEq(mockRDD), mockAny[(TaskContext, Iterator[String]) => Unit])(mockAny(classOf[ClassTag[Unit]]))

    // Note: current implementation make following code not compilable
    //       so not negative test for this case
    //  val rdd: RDD[T] = ...   // T is not a (K, V) tuple
    //  rdd.saveToGeode(regionPath)
  }
  
}
