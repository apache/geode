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
package ittest.org.apache.geode.spark.connector

import java.util.Properties

import org.apache.geode.spark.connector._
import org.apache.geode.cache.Region
import org.apache.geode.spark.connector.internal.DefaultGeodeConnectionManager
import ittest.org.apache.geode.spark.connector.testkit.GeodeCluster
import ittest.org.apache.geode.spark.connector.testkit.IOUtils
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import java.util.{HashMap => JHashMap}

class RDDJoinRegionIntegrationTest extends FunSuite with Matchers with BeforeAndAfterAll with GeodeCluster {

  var sc: SparkContext = null
  val numServers = 3
  val numObjects = 1000

  override def beforeAll() {
    // start geode cluster, and spark context
    val settings = new Properties()
    settings.setProperty("cache-xml-file", "src/it/resources/test-retrieve-regions.xml")
    settings.setProperty("num-of-servers", numServers.toString)
    val locatorPort = GeodeCluster.start(settings)

    // start spark context in local mode
    IOUtils.configTestLog4j("ERROR", "log4j.logger.org.apache.spark" -> "INFO",
      "log4j.logger.org.apache.geode.spark.connector" -> "DEBUG")
    val conf = new SparkConf()
      .setAppName("RDDJoinRegionIntegrationTest")
      .setMaster("local[2]")
      .set(GeodeLocatorPropKey, s"localhost[$locatorPort]")
    sc = new SparkContext(conf)
  }

  override def afterAll() {
    // stop connection, spark context, and geode cluster
    DefaultGeodeConnectionManager.closeConnection(GeodeConnectionConf(sc.getConf))
    sc.stop()
    GeodeCluster.stop()
  }

//  def matchMaps[K,V](map1:Map[K,V], map2:Map[K,V]) = {
//    assert(map1.size == map2.size)
//    map1.foreach(e => {
//      assert(map2.contains(e._1))
//      assert (e._2 == map2.get(e._1).get)
//    })
//  }
  
  // -------------------------------------------------------------------------------------------- 
  // PairRDD.joinGeodeRegion[K2 <: K, V2](regionPath, connConf): GeodeJoinRDD[(K, V), K, V2]
  // -------------------------------------------------------------------------------------------- 

  test("PairRDD.joinGeodeRegion: RDD[K, V] with Region[K, V2], replicated region", JoinTest) {
    verifyPairRDDJoinRegionWithSameKeyType("rr_str_int_region")
  }

  test("PairRDD.joinGeodeRegion: RDD[K, V] with Region[K, V2], partitioned region", JoinTest) {
    verifyPairRDDJoinRegionWithSameKeyType("pr_str_int_region")
  }

  test("PairRDD.joinGeodeRegion: RDD[K, V] with Region[K, V2], partitioned redundant region", JoinTest) {
    verifyPairRDDJoinRegionWithSameKeyType("pr_r_str_int_region")
  }

  def verifyPairRDDJoinRegionWithSameKeyType(regionPath: String): Unit = {
    val entriesMap: JHashMap[String, Int] = new JHashMap()
    (0 until numObjects).map(i => entriesMap.put("k_" + i, i))

    val connConf: GeodeConnectionConf = GeodeConnectionConf(sc.getConf)
    val conn = connConf.getConnection
    val rgn: Region[String, Int] = conn.getRegionProxy(regionPath)
    rgn.removeAll(rgn.keySetOnServer())
    rgn.putAll(entriesMap)

    val data = (-5 until 50).map(x => ("k_" + x, x*2))
    val rdd = sc.parallelize(data)

    val rdd2 = rdd.joinGeodeRegion[String, Int](regionPath, connConf)
    val rdd2Content = rdd2.collect()

    val expectedMap = (0 until 50).map(i => ((s"k_$i", i*2), i)).toMap
    // matchMaps[(String, Int), Int](expectedMap, rdd2Content.toMap)
    assert(expectedMap == rdd2Content.toMap)
  }

  // ------------------------------------------------------------------------------------------------------
  // PairRDD.joinGeodeRegion[K2, V2](regionPath, ((K, V)) => K2, connConf): GeodeJoinRDD[(K, V), K2, V2]
  // -------------------------------------------------------------------------------------------------------

  test("PairRDD.joinGeodeRegion: RDD[K, V] with Region[K2, V2], replicated region", JoinTest) {
    verifyPairRDDJoinRegionWithDiffKeyType("rr_str_int_region")
  }

  test("PairRDD.joinGeodeRegion: RDD[K, V] with Region[K2, V2], partitioned region", JoinTest) {
    verifyPairRDDJoinRegionWithDiffKeyType("pr_str_int_region")
  }

  test("PairRDD.joinGeodeRegion: RDD[K, V] with Region[K2, V2], partitioned redundant region", JoinTest) {
    verifyPairRDDJoinRegionWithDiffKeyType("pr_r_str_int_region")
  }

  def verifyPairRDDJoinRegionWithDiffKeyType(regionPath: String): Unit = {
    val entriesMap: JHashMap[String, Int] = new JHashMap()
    (0 until numObjects).map(i => entriesMap.put("k_" + i, i))

    val connConf: GeodeConnectionConf = GeodeConnectionConf(sc.getConf)
    val conn = connConf.getConnection
    val rgn: Region[String, Int] = conn.getRegionProxy(regionPath)
    rgn.removeAll(rgn.keySetOnServer())
    rgn.putAll(entriesMap)

    val data = (-5 until 50).map(x => (x, x*2))
    val rdd = sc.parallelize(data)

    val func :((Int, Int)) => String = pair => s"k_${pair._1}"

    val rdd2 = rdd.joinGeodeRegion[String, Int](regionPath, func /*, connConf*/)
    val rdd2Content = rdd2.collect()

    val expectedMap = (0 until 50).map(i => ((i, i*2), i)).toMap
    // matchMaps[(Int, Int), Int](expectedMap, rdd2Content.toMap)
    assert(expectedMap == rdd2Content.toMap)
  }

  // ------------------------------------------------------------------------------------------------ 
  // PairRDD.outerJoinGeodeRegion[K2 <: K, V2](regionPath, connConf): GeodeJoinRDD[(K, V), K, V2]
  // ------------------------------------------------------------------------------------------------ 

  test("PairRDD.outerJoinGeodeRegion: RDD[K, V] with Region[K, V2], replicated region", OuterJoinTest) {
    verifyPairRDDOuterJoinRegionWithSameKeyType("rr_str_int_region")
  }

  test("PairRDD.outerJoinGeodeRegion: RDD[K, V] with Region[K, V2], partitioned region", OuterJoinTest) {
    verifyPairRDDOuterJoinRegionWithSameKeyType("pr_str_int_region")
  }

  test("PairRDD.outerJoinGeodeRegion: RDD[K, V] with Region[K, V2], partitioned redundant region", OuterJoinTest) {
    verifyPairRDDOuterJoinRegionWithSameKeyType("pr_r_str_int_region")
  }

  def verifyPairRDDOuterJoinRegionWithSameKeyType(regionPath: String): Unit = {
    val entriesMap: JHashMap[String, Int] = new JHashMap()
    (0 until numObjects).map(i => entriesMap.put("k_" + i, i))

    val connConf: GeodeConnectionConf = GeodeConnectionConf(sc.getConf)
    val conn = connConf.getConnection
    val rgn: Region[String, Int] = conn.getRegionProxy(regionPath)
    rgn.removeAll(rgn.keySetOnServer())
    rgn.putAll(entriesMap)

    val data = (-5 until 50).map(x => ("k_" + x, x*2))
    val rdd = sc.parallelize(data)

    val rdd2 = rdd.outerJoinGeodeRegion[String, Int](regionPath /*, connConf*/)
    val rdd2Content = rdd2.collect()

    val expectedMap = (-5 until 50).map {
      i => if (i < 0) ((s"k_$i", i * 2), None)
      else ((s"k_$i", i*2), Some(i))}.toMap
    // matchMaps[(String, Int), Option[Int]](expectedMap, rdd2Content.toMap)
    assert(expectedMap == rdd2Content.toMap)
  }

  // ------------------------------------------------------------------------------------------------------
  // PairRDD.joinGeodeRegion[K2, V2](regionPath, ((K, V)) => K2, connConf): GeodeJoinRDD[(K, V), K2, V2]
  // -------------------------------------------------------------------------------------------------------

  test("PairRDD.outerJoinGeodeRegion: RDD[K, V] with Region[K2, V2], replicated region", OuterJoinTest) {
    verifyPairRDDOuterJoinRegionWithDiffKeyType("rr_str_int_region")
  }

  test("PairRDD.outerJoinGeodeRegion: RDD[K, V] with Region[K2, V2], partitioned region", OuterJoinTest) {
    verifyPairRDDOuterJoinRegionWithDiffKeyType("pr_str_int_region")
  }

  test("PairRDD.outerJoinGeodeRegion: RDD[K, V] with Region[K2, V2], partitioned redundant region", OuterJoinTest) {
    verifyPairRDDOuterJoinRegionWithDiffKeyType("pr_r_str_int_region")
  }

  def verifyPairRDDOuterJoinRegionWithDiffKeyType(regionPath: String): Unit = {
    val entriesMap: JHashMap[String, Int] = new JHashMap()
    (0 until numObjects).map(i => entriesMap.put("k_" + i, i))

    val connConf: GeodeConnectionConf = GeodeConnectionConf(sc.getConf)
    val conn = connConf.getConnection
    val rgn: Region[String, Int] = conn.getRegionProxy(regionPath)
    rgn.removeAll(rgn.keySetOnServer())
    rgn.putAll(entriesMap)

    val data = (-5 until 50).map(x => (x, x*2))
    val rdd = sc.parallelize(data)

    val func :((Int, Int)) => String = pair => s"k_${pair._1}"

    val rdd2 = rdd.outerJoinGeodeRegion[String, Int](regionPath, func, connConf)
    val rdd2Content = rdd2.collect()

    val expectedMap = (-5 until 50).map {
      i => if (i < 0) ((i, i * 2), None)
      else ((i, i*2), Some(i))}.toMap
    // matchMaps[(Int, Int), Option[Int]](expectedMap, rdd2Content.toMap)
    assert(expectedMap == rdd2Content.toMap)
  }

  // --------------------------------------------------------------------------------------------
  // RDD.joinGeodeRegion[K, V](regionPath, T => K,  connConf): GeodeJoinRDD[T, K, V]
  // --------------------------------------------------------------------------------------------

  test("RDD.joinGeodeRegion: RDD[T] with Region[K, V], replicated region", JoinTest) {
    verifyRDDJoinRegion("rr_str_int_region")
  }

  test("RDD.joinGeodeRegion: RDD[T] with Region[K, V], partitioned region", JoinTest) {
    verifyRDDJoinRegion("pr_str_int_region")
  }

  test("RDD.joinGeodeRegion: RDD[T] with Region[K, V], partitioned redundant region", JoinTest) {
    verifyRDDJoinRegion("pr_r_str_int_region")
  }

  def verifyRDDJoinRegion(regionPath: String): Unit = {
    val entriesMap: JHashMap[String, Int] = new JHashMap()
    (0 until numObjects).map(i => entriesMap.put("k_" + i, i))

    val connConf: GeodeConnectionConf = GeodeConnectionConf(sc.getConf)
    val conn = connConf.getConnection
    val rgn: Region[String, Int] = conn.getRegionProxy(regionPath)
    rgn.removeAll(rgn.keySetOnServer())
    rgn.putAll(entriesMap)

    val data = (-5 until 50).map(x => s"k_$x")
    val rdd = sc.parallelize(data)

    val rdd2 = rdd.joinGeodeRegion[String, Int](regionPath, x => x, connConf)
    val rdd2Content = rdd2.collect()

    val expectedMap = (0 until 50).map(i => (s"k_$i", i)).toMap
    // matchMaps[String, Int](expectedMap, rdd2Content.toMap)
    assert(expectedMap == rdd2Content.toMap)
  }

  // --------------------------------------------------------------------------------------------
  // RDD.outerJoinGeodeRegion[K, V](regionPath, T => K, connConf): GeodeJoinRDD[T, K, V]
  // --------------------------------------------------------------------------------------------

  test("RDD.outerJoinGeodeRegion: RDD[T] with Region[K, V], replicated region", OnlyTest) {
    verifyRDDOuterJoinRegion("rr_str_int_region")
  }

  test("RDD.outerJoinGeodeRegion: RDD[T] with Region[K, V], partitioned region", OnlyTest) {
    verifyRDDOuterJoinRegion("pr_str_int_region")
  }

  test("RDD.outerJoinGeodeRegion: RDD[T] with Region[K, V], partitioned redundant region", OnlyTest) {
    verifyRDDOuterJoinRegion("pr_r_str_int_region")
  }

  def verifyRDDOuterJoinRegion(regionPath: String): Unit = {
    val entriesMap: JHashMap[String, Int] = new JHashMap()
    (0 until numObjects).map(i => entriesMap.put("k_" + i, i))

    val connConf: GeodeConnectionConf = GeodeConnectionConf(sc.getConf)
    val conn = connConf.getConnection
    val rgn: Region[String, Int] = conn.getRegionProxy(regionPath)
    rgn.removeAll(rgn.keySetOnServer())
    rgn.putAll(entriesMap)

    val data = (-5 until 50).map(x => s"k_$x")
    val rdd = sc.parallelize(data)

    val rdd2 = rdd.outerJoinGeodeRegion[String, Int](regionPath, x => x /*, connConf */)
    val rdd2Content = rdd2.collect()

    val expectedMap = (-5 until 50).map {
      i => if (i < 0) (s"k_$i", None)
           else (s"k_$i", Some(i))}.toMap
    // matchMaps[String, Option[Int]](expectedMap, rdd2Content.toMap)
    assert(expectedMap == rdd2Content.toMap)
  }
  
}
