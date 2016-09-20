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
import org.apache.geode.cache.query.QueryService
import org.apache.geode.cache.query.internal.StructImpl
import org.apache.geode.spark.connector._
import org.apache.geode.cache.Region
import org.apache.geode.spark.connector.internal.{RegionMetadata, DefaultGeodeConnectionManager}
import org.apache.geode.spark.connector.internal.oql.{RDDConverter, QueryRDD}
import ittest.org.apache.geode.spark.connector.testkit.GeodeCluster
import ittest.org.apache.geode.spark.connector.testkit.IOUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, TestInputDStream}
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import scala.collection.JavaConversions
import scala.reflect.ClassTag

case class Number(str: String, len: Int)

class BasicIntegrationTest extends FunSuite with Matchers with BeforeAndAfterAll with GeodeCluster {

  var sc: SparkContext = null

  override def beforeAll() {
    // start geode cluster, and spark context
    val settings = new Properties()
    settings.setProperty("cache-xml-file", "src/it/resources/test-regions.xml")
    settings.setProperty("num-of-servers", "2")
    val locatorPort = GeodeCluster.start(settings)

    // start spark context in local mode
    IOUtils.configTestLog4j("ERROR", "log4j.logger.org.apache.spark" -> "INFO",
                            "log4j.logger.org.apache.geode.spark.connector" -> "DEBUG")
    val conf = new SparkConf()
      .setAppName("BasicIntegrationTest")
      .setMaster("local[2]")
      .set("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")
      .set(GeodeLocatorPropKey, s"localhost[$locatorPort]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.apache.geode.spark.connector.GeodeKryoRegistrator")

    sc = new SparkContext(conf)
  }

  override def afterAll() {
    // stop connection, spark context, and geode cluster
    DefaultGeodeConnectionManager.closeConnection(GeodeConnectionConf(sc.getConf))
    sc.stop()
    GeodeCluster.stop()
  }

  //Convert Map[Object, Object] to java.util.Properties
  private def map2Props(map: Map[Object, Object]): java.util.Properties =
    (new java.util.Properties /: map) {case (props, (k,v)) => props.put(k,v); props}

  // ===========================================================
  //       DefaultGeodeConnection functional tests
  // ===========================================================

  test("DefaultGeodeConnection.validateRegion()") {
    val conn = GeodeConnectionConf(sc.getConf).getConnection

    // normal exist-region
    var regionPath: String = "str_str_region"
    conn.validateRegion[String, String](regionPath)

    // non-exist region
    regionPath = "non_exist_region"
    try {
      conn.validateRegion[String, String](regionPath)
      fail("validateRegion failed to catch non-exist region error")
    } catch {
      case e: RuntimeException => 
        if (! e.getMessage.contains(s"The region named $regionPath was not found"))
          fail("validateRegion gives wrong exception on non-exist region", e)
      case e: Throwable => 
        fail("validateRegion gives wrong exception on non-exist region", e)
    }

    // Note: currently, can't catch type mismatch error
    conn.validateRegion[String, Integer]("str_str_region")
  }

  test("DefaultGeodeConnection.getRegionMetadata()") {
    val conn = GeodeConnectionConf(sc.getConf).getConnection

    // exist region
    validateRegionMetadata(conn, "obj_obj_region", true, 113, null, null, false)
    validateRegionMetadata(conn, "str_int_region", true, 113, "java.lang.String", "java.lang.Integer", false)
    validateRegionMetadata(conn, "str_str_rep_region", false, 0, "java.lang.String", "java.lang.String", true)

    // non-exist region
    assert(! conn.getRegionMetadata("no_exist_region").isDefined)
  }
    
  def validateRegionMetadata(
    conn: GeodeConnection, regionPath: String, partitioned: Boolean, buckets: Int,
    keyType: String, valueType: String, emptyMap: Boolean): Unit = {

    val mdOption = conn.getRegionMetadata(regionPath)
    val md = mdOption.get
   
    assert(md.getRegionPath == s"/$regionPath")
    assert(md.isPartitioned == partitioned)
    assert(md.getKeyTypeName == keyType)
    assert(md.getValueTypeName == valueType)
    assert(md.getTotalBuckets == buckets)
    if (emptyMap) assert(md.getServerBucketMap == null) 
    else assert(md.getServerBucketMap != null)
  }

  test("DefaultGeodeConnection.getRegionProxy()") {
    val conn = GeodeConnectionConf(sc.getConf).getConnection

    val region1 = conn.getRegionProxy[String, String]("str_str_region")
    region1.put("1", "One")
    assert(region1.get("1") == "One")
    region1.remove("1")
    assert(region1.get("1") == null)
    
    // getRegionProxy doesn't fail when region doesn't exist
    val region2 = conn.getRegionProxy[String, String]("non_exist_region")
    try {
      region2.put("1", "One")
      fail("getRegionProxy failed to catch non-exist region error")
    } catch {
      case e: Exception =>
        if (e.getCause == null || ! e.getCause.getMessage.contains(s"Region named /non_exist_region was not found")) {
          e.printStackTrace()
          fail("validateRegion gives wrong exception on non-exist region", e)
        }
    }
  }
  
  // Note: DefaultGeodeConnecton.getQuery() and getRegionData() are covered by
  //       RetrieveRegionIntegrationTest.scala and following OQL tests.
  
  // ===========================================================
  //                OQL functional tests
  // ===========================================================
  
  private def initRegion(regionName: String): Unit = {

    //Populate some data in the region
    val conn = GeodeConnectionConf(sc.getConf).getConnection
    val rgn: Region[Object, Object] = conn.getRegionProxy(regionName)
    rgn.removeAll(rgn.keySetOnServer())

    //This will call the implicit conversion map2Properties in connector package object, since it is Map[String, String]
    var position1 = new Position(Map("secId" -> "SUN", "qty" -> "34000", "mktValue" -> "24.42"))
    var position2 = new Position(Map("secId" -> "IBM", "qty" -> "8765", "mktValue" -> "34.29"))
    val portfolio1 = new Portfolio(map2Props(Map("id" ->"1", "type" -> "type1", "status" -> "active",
      "position1" -> position1, "position2" -> position2)))
    rgn.put("1", portfolio1)

    position1 = new Position(Map("secId" -> "YHOO", "qty" -> "9834", "mktValue" -> "12.925"))
    position2 = new Position(Map("secId" -> "GOOG", "qty" -> "12176", "mktValue" -> "21.972"))
    val portfolio2 = new Portfolio(map2Props(Map("id" -> "2", "type" -> "type2", "status" -> "inactive",
      "position1" -> position1, "position2" -> position2)))
    rgn.put("2", portfolio2)

    position1 = new Position(Map("secId" -> "MSFT", "qty" -> "98327", "mktValue" -> "23.32"))
    position2 = new Position(Map("secId" -> "AOL", "qty" -> "978", "mktValue" -> "40.373"))
    val portfolio3 = new Portfolio(map2Props(Map("id" -> "3", "type" -> "type3", "status" -> "active",
      "position1" -> position1, "position2" -> position2)))
    rgn.put("3", portfolio3)

    position1 = new Position(Map("secId" -> "APPL", "qty" -> "67", "mktValue" -> "67.356572"))
    position2 = new Position(Map("secId" -> "ORCL", "qty" -> "376", "mktValue" -> "101.34"))
    val portfolio4 = new Portfolio(map2Props(Map("id" -> "4", "type" -> "type1", "status" -> "inactive",
      "position1" -> position1, "position2" -> position2)))
    rgn.put("4", portfolio4)

    position1 = new Position(Map("secId" -> "SAP", "qty" -> "90", "mktValue" -> "67.356572"))
    position2 = new Position(Map("secId" -> "DELL", "qty" -> "376", "mktValue" -> "101.34"))
    val portfolio5 = new Portfolio(map2Props(Map("id" -> "5", "type" -> "type2", "status" -> "active",
      "position1" -> position1, "position2" -> position2)))
    rgn.put("5", portfolio5)

    position1 = new Position(Map("secId" -> "RHAT", "qty" -> "90", "mktValue" -> "67.356572"))
    position2 = new Position(Map("secId" -> "NOVL", "qty" -> "376", "mktValue" -> "101.34"))
    val portfolio6 = new Portfolio(map2Props(Map("id" -> "6", "type" -> "type3", "status" -> "inactive",
      "position1" -> position1, "position2" -> position2)))
    rgn.put("6", portfolio6)

    position1 = new Position(Map("secId" -> "MSFT", "qty" -> "98327", "mktValue" -> "23.32"))
    position2 = new Position(Map("secId" -> "AOL", "qty" -> "978", "mktValue" -> "40.373"))
    val portfolio7 = new Portfolio(map2Props(Map("id" -> "7", "type" -> "type4", "status" -> "active",
      "position1" -> position1, "position2" -> position2)))
    //Not using null, due to intermittent query failure on column containing null, likely a Spark SQL bug
    //portfolio7.setType(null)
    rgn.put("7", portfolio7)
  }

  private def getQueryRDD[T: ClassTag](
    query: String, connConf: GeodeConnectionConf = GeodeConnectionConf(sc.getConf)): QueryRDD[T] =
      new QueryRDD[T](sc, query, connConf)

  test("Run Geode OQL query and convert the returned QueryRDD to DataFrame: Partitioned Region") {
    simpleQuery("obj_obj_region")
  }

  test("Run Geode OQL query and convert the returned QueryRDD to DataFrame: Replicated Region") {
    simpleQuery("obj_obj_rep_region")
  }

  private def simpleQuery(regionName: String) {
    //Populate some data in the region
    val connConf: GeodeConnectionConf = GeodeConnectionConf(sc.getConf)
    val conn = connConf.getConnection
    val rgn: Region[String, String] = conn.getRegionProxy(regionName)
    rgn.removeAll(rgn.keySetOnServer())
    rgn.putAll(JavaConversions.mapAsJavaMap(Map("1" -> "one", "2" -> "two", "3" -> "three")))

    //Create QueryRDD using OQL
    val OQLResult: QueryRDD[String] = getQueryRDD[String](s"select * from /$regionName")

    //verify the QueryRDD
    val oqlRS: Array[String] = OQLResult.collect()
    oqlRS should have length 3
    oqlRS should contain theSameElementsAs List("one", "two", "three")

    //Convert QueryRDD to DataFrame
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._
    val dataFrame = OQLResult.map(x => Number(x, x.length)).toDF()
    //Register dataFrame as a table of two columns of type String and Int respectively
    dataFrame.registerTempTable("numberTable")

    //Issue SQL query against the table
    val SQLResult = sqlContext.sql("SELECT * FROM numberTable")
    //Verify the SQL query result, r(0) mean column 0
    val sqlRS: Array[Any] = SQLResult.map(r => r(0)).collect()
    sqlRS should have length 3
    sqlRS should contain theSameElementsAs List("one", "two", "three")

    //Convert QueryRDD to DataFrame using RDDConverter
    val dataFrame2 = RDDConverter.queryRDDToDataFrame(OQLResult, sqlContext)
    //Register dataFrame2 as a table of two columns of type String and Int respectively
    dataFrame2.registerTempTable("numberTable2")

    //Issue SQL query against the table
    val SQLResult2 = sqlContext.sql("SELECT * FROM numberTable2")
    //Verify the SQL query result, r(0) mean column 0
    val sqlRS2: Array[Any] = SQLResult2.map(r => r(0)).collect()
    sqlRS2 should have length 3
    sqlRS2 should contain theSameElementsAs List("one", "two", "three")

    //Remove the region entries, because other tests might use the same region as well
    List("1", "2", "3").foreach(rgn.remove)
  }

  test("Run Geode OQL query and directly return DataFrame: Partitioned Region") {
    simpleQueryDataFrame("obj_obj_region")
  }

  test("Run Geode OQL query and directly return DataFrame: Replicated Region") {
    simpleQueryDataFrame("obj_obj_rep_region")
  }

  private def simpleQueryDataFrame(regionName: String) {
    //Populate some data in the region
    val conn = GeodeConnectionConf(sc.getConf).getConnection
    val rgn: Region[String, String] = conn.getRegionProxy(regionName)
    rgn.removeAll(rgn.keySetOnServer())
    rgn.putAll(JavaConversions.mapAsJavaMap(Map("1" -> "one", "2" -> "two", "3" -> "three")))

    //Create DataFrame using Geode OQL
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val dataFrame = sqlContext.geodeOQL(s"select * from /$regionName")
    dataFrame.registerTempTable("numberTable")

    //Issue SQL query against the table
    val SQLResult = sqlContext.sql("SELECT * FROM numberTable")
    //Verify the SQL query result, r(0) mean column 0
    val sqlRS: Array[Any] = SQLResult.map(r => r(0)).collect()
    sqlRS should have length 3
    sqlRS should contain theSameElementsAs List("one", "two", "three")

    //Remove the region entries, because other tests might use the same region as well
    List("1", "2", "3").foreach(rgn.remove)
  }

  test("Geode OQL query with UDT: Partitioned Region") {
    queryUDT("obj_obj_region")
  }

  test("Geode OQL query with UDT: Replicated Region") {
    queryUDT("obj_obj_rep_region")
  }

  private def queryUDT(regionName: String) {

    //Populate some data in the region
    val conn = GeodeConnectionConf(sc.getConf).getConnection
    val rgn: Region[Object, Object] = conn.getRegionProxy(regionName)
    rgn.removeAll(rgn.keySetOnServer())
    val e1: Employee = new Employee("hello", 123)
    val e2: Employee = new Employee("world", 456)
    rgn.putAll(JavaConversions.mapAsJavaMap(Map("1" -> e1, "2" -> e2)))

    //Create QueryRDD using OQL
    val OQLResult: QueryRDD[Object] = getQueryRDD(s"select name, age from /$regionName")

    //verify the QueryRDD
    val oqlRS: Array[Object] = OQLResult.collect()
    oqlRS should have length 2
    oqlRS.map(e => e.asInstanceOf[StructImpl].getFieldValues.apply(1)) should contain theSameElementsAs List(123, 456)

    //Convert QueryRDD to DataFrame
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //Convert QueryRDD to DataFrame using RDDConverter
    val dataFrame = RDDConverter.queryRDDToDataFrame(OQLResult, sqlContext)
    dataFrame.registerTempTable("employee")
    val SQLResult = sqlContext.sql("SELECT * FROM employee")

    //Verify the SQL query result
    val sqlRS = SQLResult.map(r => r(0)).collect()
    sqlRS should have length 2
    sqlRS should contain theSameElementsAs List("hello", "world")

    List("1", "2").foreach(rgn.remove)
  }

  test("Geode OQL query with UDT and directly return DataFrame: Partitioned Region") {
    queryUDTDataFrame("obj_obj_region")
  }

  test("Geode OQL query with UDT and directly return DataFrame: Replicated Region") {
    queryUDTDataFrame("obj_obj_rep_region")
  }

  private def queryUDTDataFrame(regionName: String) {
    //Populate some data in the region
    val conn = GeodeConnectionConf(sc.getConf).getConnection
    val rgn: Region[Object, Object] = conn.getRegionProxy(regionName)
    rgn.removeAll(rgn.keySetOnServer())
    val e1: Employee = new Employee("hello", 123)
    val e2: Employee = new Employee("world", 456)
    rgn.putAll(JavaConversions.mapAsJavaMap(Map("1" -> e1, "2" -> e2)))

    //Create DataFrame using Geode OQL
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val dataFrame = sqlContext.geodeOQL(s"select name, age from /$regionName")

    dataFrame.registerTempTable("employee")
    val SQLResult = sqlContext.sql("SELECT * FROM employee")

    //Verify the SQL query result
    val sqlRS = SQLResult.map(r => r(0)).collect()
    sqlRS should have length 2
    sqlRS should contain theSameElementsAs List("hello", "world")

    List("1", "2").foreach(rgn.remove)
  }

  test("Geode OQL query with more complex UDT: Partitioned Region") {
    complexUDT("obj_obj_region")
  }

  test("Geode OQL query with more complex UDT: Replicated Region") {
    complexUDT("obj_obj_rep_region")
  }

  private def complexUDT(regionName: String) {

    initRegion(regionName)

    //Create QueryRDD using OQL
    val OQLResult: QueryRDD[Object] = getQueryRDD(s"SELECT DISTINCT * FROM /$regionName WHERE status = 'active'")

    //verify the QueryRDD
    val oqlRS: Array[Int] = OQLResult.collect().map(r => r.asInstanceOf[Portfolio].getId)
    oqlRS should contain theSameElementsAs List(1, 3, 5, 7)

    //Convert QueryRDD to DataFrame
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //Convert QueryRDD to DataFrame using RDDConverter
    val dataFrame = RDDConverter.queryRDDToDataFrame(OQLResult, sqlContext)

    dataFrame.registerTempTable("Portfolio")

    val SQLResult = sqlContext.sql("SELECT * FROM Portfolio")

    //Verify the SQL query result
    val sqlRS = SQLResult.collect().map(r => r(0).asInstanceOf[Portfolio].getType)
    sqlRS should contain theSameElementsAs List("type1", "type2", "type3", "type4")
  }

  test("Geode OQL query with more complex UDT and directly return DataFrame: Partitioned Region") {
    complexUDTDataFrame("obj_obj_region")
  }

  test("Geode OQL query with more complex UDT and directly return DataFrame: Replicated Region") {
    complexUDTDataFrame("obj_obj_rep_region")
  }

  private def complexUDTDataFrame(regionName: String) {

    initRegion(regionName)

    //Create DataFrame using Geode OQL
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val dataFrame = sqlContext.geodeOQL(s"SELECT DISTINCT * FROM /$regionName WHERE status = 'active'")
    dataFrame.registerTempTable("Portfolio")

    val SQLResult = sqlContext.sql("SELECT * FROM Portfolio")

    //Verify the SQL query result
    val sqlRS = SQLResult.collect().map(r => r(0).asInstanceOf[Portfolio].getType)
    sqlRS should contain theSameElementsAs List("type1", "type2", "type3", "type4")
  }

  test("Geode OQL query with more complex UDT with Projection: Partitioned Region") {
    queryComplexUDTProjection("obj_obj_region")
  }

  test("Geode OQL query with more complex UDT with Projection: Replicated Region") {
    queryComplexUDTProjection("obj_obj_rep_region")
  }

  private def queryComplexUDTProjection(regionName: String) {

    initRegion(regionName)

    //Create QueryRDD using OQL
    val OQLResult: QueryRDD[Object] = getQueryRDD[Object](s"""SELECT id, "type", positions, status FROM /$regionName WHERE status = 'active'""")

    //verify the QueryRDD
    val oqlRS: Array[Int] = OQLResult.collect().map(si => si.asInstanceOf[StructImpl].getFieldValues.apply(0).asInstanceOf[Int])
    oqlRS should contain theSameElementsAs List(1, 3, 5, 7)

    //Convert QueryRDD to DataFrame
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //Convert QueryRDD to DataFrame using RDDConverter
    val dataFrame = RDDConverter.queryRDDToDataFrame(OQLResult, sqlContext)

    dataFrame.registerTempTable("Portfolio")

    val SQLResult = sqlContext.sql("SELECT id, type FROM Portfolio where type = 'type3'")

    //Verify the SQL query result
    val sqlRS = SQLResult.collect().map(r => r(0))
    sqlRS should contain theSameElementsAs List(3)
  }

  test("Geode OQL query with more complex UDT with Projection and directly return DataFrame: Partitioned Region") {
    queryComplexUDTProjectionDataFrame("obj_obj_region")
  }

  test("Geode OQL query with more complex UDT with Projection and directly return DataFrame: Replicated Region") {
    queryComplexUDTProjectionDataFrame("obj_obj_rep_region")
  }

  private def queryComplexUDTProjectionDataFrame(regionName: String) {

    initRegion(regionName)

    //Create DataFrame using Geode OQL
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val dataFrame = sqlContext.geodeOQL(s"""SELECT id, "type", positions, status FROM /$regionName WHERE status = 'active'""")
    dataFrame.registerTempTable("Portfolio")

    val SQLResult = sqlContext.sql("SELECT id, type FROM Portfolio where type = 'type3'")

    //Verify the SQL query result
    val sqlRS = SQLResult.collect().map(r => r(0))
    sqlRS should contain theSameElementsAs List(3)
  }

  test("Geode OQL query with more complex UDT with nested Projection and directly return DataFrame: Partitioned Region") {
    queryComplexUDTNestProjectionDataFrame("obj_obj_region")
  }

  test("Geode OQL query with more complex UDT with nested Projection and directly return DataFrame: Replicated Region") {
    queryComplexUDTNestProjectionDataFrame("obj_obj_rep_region")
  }

  private def queryComplexUDTNestProjectionDataFrame(regionName: String) {

    initRegion(regionName)

    //Create DataFrame using Geode OQL
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val dataFrame = sqlContext.geodeOQL(s"""SELECT r.id, r."type", r.positions, r.status FROM /$regionName r, r.positions.values f WHERE r.status = 'active' and f.secId = 'MSFT'""")
    dataFrame.registerTempTable("Portfolio")

    val SQLResult = sqlContext.sql("SELECT id, type FROM Portfolio where type = 'type3'")

    //Verify the SQL query result
    val sqlRS = SQLResult.collect().map(r => r(0))
    sqlRS should contain theSameElementsAs List(3)
  }

  test("Undefined instance deserialization: Partitioned Region") {
    undefinedInstanceDeserialization("obj_obj_region")
  }

  test("Undefined instance deserialization: Replicated Region") {
    undefinedInstanceDeserialization("obj_obj_rep_region")
  }

  private def undefinedInstanceDeserialization(regionName: String) {

    val conn = GeodeConnectionConf(sc.getConf).getConnection
    val rgn: Region[Object, Object] = conn.getRegionProxy(regionName)
    rgn.removeAll(rgn.keySetOnServer())

    //Put some new data
    rgn.put("1", "one")

    //Query some non-existent columns, which should return UNDEFINED
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val dataFrame = sqlContext.geodeOQL(s"SELECT col100, col200 FROM /$regionName")
    val col1 = dataFrame.first().apply(0)
    val col2 = dataFrame.first().apply(1)
    assert(col1 == QueryService.UNDEFINED)
    assert(col2 == QueryService.UNDEFINED)
    //Verify that col1 and col2 refer to the same Undefined object
    assert(col1.asInstanceOf[AnyRef] eq col2.asInstanceOf[AnyRef])
  }

  test("RDD.saveToGeode") {
    val regionName = "str_str_region"
    // generate: Vector((1,11), (2,22), (3,33), (4,44), (5,55), (6,66))
    val data = (1 to 6).map(_.toString).map(e=> (e, e*2))
    val rdd = sc.parallelize(data)
    rdd.saveToGeode(regionName)

    // verify
    val connConf: GeodeConnectionConf = GeodeConnectionConf(sc.getConf)
    val region: Region[String, String] = connConf.getConnection.getRegionProxy(regionName)
    println("region key set on server: " + region.keySetOnServer())
    assert((1 to 6).map(_.toString).toSet == JavaConversions.asScalaSet(region.keySetOnServer()))
    (1 to 6).map(_.toString).foreach(e => assert(e*2 == region.get(e)))
  }

  // ===========================================================
  //        DStream.saveToGeode() functional tests
  // ===========================================================

  test("Basic DStream test") {
    import org.apache.spark.streaming.scheduler.{StreamingListenerBatchCompleted, StreamingListener}
    import org.apache.geode.spark.connector.streaming._
    import org.apache.spark.streaming.ManualClockHelper

    class TestStreamListener extends StreamingListener {
      var count = 0
      override def onBatchCompleted(batch: StreamingListenerBatchCompleted) = count += 1
    }

    def batchDuration = Seconds(1)
    val ssc = new StreamingContext(sc, batchDuration)
    val input = Seq(1 to 4, 5 to 8, 9 to 12)
    val dstream = new TestInputDStream(ssc, input, 2)
    dstream.saveToGeode[String, Int]("str_int_region", (e: Int) => (e.toString, e))
    try {
      val listener = new TestStreamListener
      ssc.addStreamingListener(listener)
      ssc.start()
      ManualClockHelper.addToTime(ssc, batchDuration.milliseconds * input.length)
      while (listener.count < input.length) ssc.awaitTerminationOrTimeout(50)
    } catch {
      case e: Exception => e.printStackTrace(); throw e
//    } finally {
//      ssc.stop()
    }

    val connConf: GeodeConnectionConf = GeodeConnectionConf(sc.getConf)
    val conn = connConf.getConnection
    val region: Region[String, Int] = conn.getRegionProxy("str_int_region")

    // verify geode region contents
    println("region key set on server: " + region.keySetOnServer())
    assert((1 to 12).map(_.toString).toSet == JavaConversions.asScalaSet(region.keySetOnServer()))
    (1 to 12).foreach(e => assert(e == region.get(e.toString)))
  }
}
