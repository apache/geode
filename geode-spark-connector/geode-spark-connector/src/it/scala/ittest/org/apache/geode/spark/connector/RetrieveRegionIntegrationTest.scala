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
import org.scalatest.{Tag, BeforeAndAfterAll, FunSuite, Matchers}
import java.util.{HashMap => JHashMap}


class RetrieveRegionIntegrationTest extends FunSuite with Matchers with BeforeAndAfterAll with GeodeCluster {

  var sc: SparkContext = null
  val numServers = 4
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
      .setAppName("RetrieveRegionIntegrationTest")
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
  
  def executeTest[K,V](regionName:String, numObjects:Int, entriesMap:java.util.Map[K,V]) = {
    //Populate some data in the region
    val connConf: GeodeConnectionConf = GeodeConnectionConf(sc.getConf)
    val conn = connConf.getConnection
    val rgn: Region[K, V] = conn.getRegionProxy(regionName)
    rgn.removeAll(rgn.keySetOnServer())
    rgn.putAll(entriesMap)
    verifyRetrieveRegion[K,V](regionName, entriesMap)
  }
    
  def verifyRetrieveRegion[K,V](regionName:String, entriesMap:java.util.Map[K,V])  = {
    val rdd = sc.geodeRegion(regionName)
    val collectedObjs = rdd.collect()
    collectedObjs should have length entriesMap.size
    import scala.collection.JavaConverters._
    matchMaps[K,V](entriesMap.asScala.toMap, collectedObjs.toMap)
  }
 
  def matchMaps[K,V](map1:Map[K,V], map2:Map[K,V]) = {
    assert(map1.size == map2.size)
    map1.foreach(e => {
      assert(map2.contains(e._1))
      assert (e._2 == map2.get(e._1).get)
      }
    )
  }
  
  //Retrieve region for Partitioned Region where some nodes are empty (empty iterator)
  //This test has to run first...the rest of the tests always use the same num objects
  test("Retrieve Region for PR where some nodes are empty (Empty Iterator)") {
    val numObjects = numServers - 1
    val entriesMap:JHashMap[String, Int] = new JHashMap()
    (0 until numObjects).map(i => entriesMap.put("key_" + i, i))
    executeTest[String, Int]("rr_str_int_region", numObjects, entriesMap)
  }

  //Test for retrieving from region containing string key and int value
  def verifyRetrieveStringStringRegion(regionName:String) = {
    val entriesMap:JHashMap[String, String] = new JHashMap()
    (0 until numObjects).map(i => entriesMap.put("key_" + i, "value_" + i))
    executeTest[String, String](regionName, numObjects, entriesMap)
  }

  test("Retrieve Region with replicate redundant string string") {
    verifyRetrieveStringStringRegion("rr_obj_obj_region")
  }

  test("Retrieve Region with partitioned string string") {
    verifyRetrieveStringStringRegion("pr_obj_obj_region")
  }

  test("Retrieve Region with partitioned redundant string string") {
    verifyRetrieveStringStringRegion("pr_r_obj_obj_region")
  }
  

  //Test for retrieving from region containing string key and string value
  def verifyRetrieveStringIntRegion(regionName:String) = {
    val entriesMap:JHashMap[String, Int] = new JHashMap()
    (0 until numObjects).map(i => entriesMap.put("key_" + i, i))
    executeTest[String, Int](regionName, numObjects, entriesMap)
  }

  test("Retrieve Region with replicate string int region") {
    verifyRetrieveStringIntRegion("rr_str_int_region")
  }

  test("Retrieve Region with partitioned string int region") {
    verifyRetrieveStringIntRegion("pr_str_int_region")
  }

  test("Retrieve Region with partitioned redundant string int region") {
    verifyRetrieveStringIntRegion("pr_r_str_int_region")
  }

  //Tests for retrieving from region containing string key and object value
  def verifyRetrieveStringObjectRegion(regionName:String) = {
    val entriesMap:JHashMap[String, Object] = new JHashMap()
    (0 until numObjects).map(i => entriesMap.put("key_" + i, new Employee("ename" + i, i)))
    executeTest[String, Object](regionName, numObjects, entriesMap)
  }

  test("Retrieve Region with replicate string obj") {
    verifyRetrieveStringObjectRegion("rr_obj_obj_region")
  }

  test("Retrieve Region with partitioned string obj") {
    verifyRetrieveStringObjectRegion("pr_obj_obj_region")
  }

  test("Retrieve Region with partitioned redundant string obj") {
    verifyRetrieveStringObjectRegion("pr_r_obj_obj_region")
  }

  //Test for retrieving from region containing string key and map value
  def verifyRetrieveStringMapRegion(regionName:String) = {
    val entriesMap:JHashMap[String,JHashMap[String,String]] = new JHashMap()
    (0 until numObjects).map(i => {
      val hashMap:JHashMap[String, String] = new JHashMap()
      hashMap.put("mapKey:" + i, "mapValue:" + i)
      entriesMap.put("key_" + i, hashMap)
    })
    executeTest(regionName, numObjects, entriesMap)
  }

  test("Retrieve Region with replicate string map region") {
    verifyRetrieveStringMapRegion("rr_obj_obj_region")
  }

  test("Retrieve Region with partitioned string map region") {
    verifyRetrieveStringMapRegion("pr_obj_obj_region")
  }

  test("Retrieve Region with partitioned redundant string map region") {
    verifyRetrieveStringMapRegion("pr_r_obj_obj_region")
  }
  
  //Test and helpers specific for retrieving from region containing string key and byte[] value
  def executeTestWithByteArrayValues[K](regionName:String, numObjects:Int, entriesMap:java.util.Map[K,Array[Byte]]) = {
    //Populate some data in the region
    val connConf: GeodeConnectionConf = GeodeConnectionConf(sc.getConf)
    val conn = connConf.getConnection
    val rgn: Region[K, Array[Byte]] = conn.getRegionProxy(regionName)
    rgn.putAll(entriesMap)
    verifyRetrieveRegionWithByteArrayValues[K](regionName, entriesMap)
  }
  
  def verifyRetrieveRegionWithByteArrayValues[K](regionName:String, entriesMap:java.util.Map[K,Array[Byte]])  = {
    val rdd = sc.geodeRegion(regionName)
    val collectedObjs = rdd.collect()
    collectedObjs should have length entriesMap.size
    import scala.collection.JavaConverters._
    matchByteArrayMaps[K](entriesMap.asScala.toMap, collectedObjs.toMap)
  }
  
  def matchByteArrayMaps[K](map1:Map[K,Array[Byte]], map2:Map[K,Array[Byte]]) = {
    map1.foreach(e => {
      assert(map2.contains(e._1))
      assert (java.util.Arrays.equals(e._2, map2.get(e._1).get))
      }
    )
    assert(map1.size == map2.size)

  }
  
  def verifyRetrieveStringByteArrayRegion(regionName:String) = {
    val entriesMap:JHashMap[String, Array[Byte]] = new JHashMap()
    (0 until numObjects).map(i => entriesMap.put("key_" + i, Array[Byte](192.toByte, 168.toByte, 0, i.toByte)))
    executeTestWithByteArrayValues[String](regionName, numObjects, entriesMap)
  }
      
  test("Retrieve Region with replicate region string byte[] region") {
    verifyRetrieveStringByteArrayRegion("rr_obj_obj_region")
  }

  test("Retrieve Region with partition region string byte[] region") {
    verifyRetrieveStringByteArrayRegion("pr_obj_obj_region")
  }

  test("Retrieve Region with partition redundant region string byte[] region") {
    verifyRetrieveStringByteArrayRegion("pr_r_obj_obj_region")
  }

  test("Retrieve Region with where clause on partitioned redundant region", FilterTest) {
    verifyRetrieveRegionWithWhereClause("pr_r_str_int_region")
  }

  test("Retrieve Region with where clause on partitioned region", FilterTest) {
    verifyRetrieveRegionWithWhereClause("pr_str_int_region")
  }

  test("Retrieve Region with where clause on replicated region", FilterTest) {
    verifyRetrieveRegionWithWhereClause("rr_str_int_region")
  }

  def verifyRetrieveRegionWithWhereClause(regionPath: String): Unit = {
    val entriesMap: JHashMap[String, Int] = new JHashMap()
    (0 until numObjects).map(i => entriesMap.put("key_" + i, i))

    val connConf: GeodeConnectionConf = GeodeConnectionConf(sc.getConf)
    val conn = connConf.getConnection
    val rgn: Region[String, Int] = conn.getRegionProxy(regionPath)
    rgn.removeAll(rgn.keySetOnServer())
    rgn.putAll(entriesMap)

    val rdd = sc.geodeRegion(regionPath).where("value.intValue() < 50")
    val expectedMap = (0 until 50).map(i => (s"key_$i", i)).toMap
    val collectedObjs = rdd.collect()
    // collectedObjs should have length expectedMap.size
    matchMaps[String, Int](expectedMap, collectedObjs.toMap)
  }

}
