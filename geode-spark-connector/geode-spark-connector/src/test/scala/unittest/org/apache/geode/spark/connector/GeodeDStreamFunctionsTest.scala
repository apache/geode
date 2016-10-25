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
import org.apache.geode.spark.connector.{GeodeConnection, GeodeConnectionConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FunSuite}
import org.mockito.Matchers.{eq => mockEq, any => mockAny}

import scala.reflect.ClassTag

class GeodeDStreamFunctionsTest extends FunSuite with Matchers with MockitoSugar {

  test("test GeodePairDStreamFunctions Implicit") {
    import org.apache.geode.spark.connector.streaming._
    val mockDStream = mock[DStream[(Int, String)]]
    // the implicit make the following line valid
    val pairDStream: GeodePairDStreamFunctions[Int, String] = mockDStream
    pairDStream shouldBe a[GeodePairDStreamFunctions[_, _]]
  }

  test("test GeodeDStreamFunctions Implicit") {
    import org.apache.geode.spark.connector.streaming._
    val mockDStream = mock[DStream[String]]
    // the implicit make the following line valid
    val dstream: GeodeDStreamFunctions[String] = mockDStream
    dstream shouldBe a[GeodeDStreamFunctions[_]]
  }

  def createMocks[K, V](regionPath: String)
    (implicit kt: ClassTag[K], vt: ClassTag[V], m: Manifest[Region[K, V]])
    : (String, GeodeConnectionConf, GeodeConnection, Region[K, V]) = {
    val mockConnection = mock[GeodeConnection]
    val mockConnConf = mock[GeodeConnectionConf]
    val mockRegion = mock[Region[K, V]]
    when(mockConnConf.getConnection).thenReturn(mockConnection)
    when(mockConnConf.locators).thenReturn(Seq.empty)
    (regionPath, mockConnConf, mockConnection, mockRegion)
  }

  test("test GeodePairDStreamFunctions.saveToGeode()") {
    import org.apache.geode.spark.connector.streaming._
    val (regionPath, mockConnConf, mockConnection, mockRegion) = createMocks[String, String]("test")
    val mockDStream = mock[DStream[(String, String)]]
    mockDStream.saveToGeode(regionPath, mockConnConf)
    verify(mockConnConf).getConnection
    verify(mockConnection).validateRegion[String, String](regionPath)
    verify(mockDStream).foreachRDD(mockAny[(RDD[(String, String)]) => Unit])
  }

  test("test GeodeDStreamFunctions.saveToGeode()") {
    import org.apache.geode.spark.connector.streaming._
    val (regionPath, mockConnConf, mockConnection, mockRegion) = createMocks[String, Int]("test")
    val mockDStream = mock[DStream[String]]
    mockDStream.saveToGeode[String, Int](regionPath,  (s: String) => (s, s.length), mockConnConf)
    verify(mockConnConf).getConnection
    verify(mockConnection).validateRegion[String, String](regionPath)
    verify(mockDStream).foreachRDD(mockAny[(RDD[String]) => Unit])
  }

}
