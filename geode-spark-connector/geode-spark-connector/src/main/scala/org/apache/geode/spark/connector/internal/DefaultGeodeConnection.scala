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
package org.apache.geode.spark.connector.internal

import java.net.InetAddress

import org.apache.geode.cache.client.{ClientCache, ClientCacheFactory, ClientRegionShortcut}
import org.apache.geode.cache.execute.{FunctionException, FunctionService}
import org.apache.geode.cache.query.Query
import org.apache.geode.cache.{Region, RegionService}
import org.apache.geode.internal.cache.execute.InternalExecution
import org.apache.geode.spark.connector.internal.oql.QueryResultCollector
import org.apache.geode.spark.connector.internal.rdd.GeodeRDDPartition
import org.apache.spark.{SparkEnv, Logging}
import org.apache.geode.spark.connector.GeodeConnection
import org.apache.geode.spark.connector.internal.geodefunctions._
import java.util.{Set => JSet, List => JList }

/**
 * Default GeodeConnection implementation. The instance of this should be
 * created by DefaultGeodeConnectionFactory
 * @param locators pairs of host/port of locators
 * @param gemFireProps The initial geode properties to be used.
 */
private[connector] class DefaultGeodeConnection (
  locators: Seq[(String, Int)], gemFireProps: Map[String, String] = Map.empty) 
  extends GeodeConnection with Logging {

  private val clientCache = initClientCache()

  /** Register Geode functions to the Geode cluster */
  FunctionService.registerFunction(RetrieveRegionMetadataFunction.getInstance())
  FunctionService.registerFunction(RetrieveRegionFunction.getInstance())

  private def initClientCache() : ClientCache = {
    try {
      val ccf = getClientCacheFactory
      ccf.create()
    } catch {
      case e: Exception =>
        logError(s"""Failed to init ClientCache, locators=${locators.mkString(",")}, Error: $e""")
        throw new RuntimeException(e)
    }
  }
  
  private def getClientCacheFactory: ClientCacheFactory = {
    import org.apache.geode.spark.connector.map2Properties
    val ccf = new ClientCacheFactory(gemFireProps)
    ccf.setPoolReadTimeout(30000)
    val servers = LocatorHelper.getAllGeodeServers(locators)
    if (servers.isDefined && servers.get.size > 0) {
      val sparkIp = System.getenv("SPARK_LOCAL_IP")
      val hostName = if (sparkIp != null) InetAddress.getByName(sparkIp).getCanonicalHostName
                     else InetAddress.getLocalHost.getCanonicalHostName
      val executorId = SparkEnv.get.executorId      
      val pickedServers = LocatorHelper.pickPreferredGeodeServers(servers.get, hostName, executorId)
      logInfo(s"""Init ClientCache: severs=${pickedServers.mkString(",")}, host=$hostName executor=$executorId props=$gemFireProps""")
      logDebug(s"""Init ClientCache: all-severs=${pickedServers.mkString(",")}""")
      pickedServers.foreach{ case (host, port)  => ccf.addPoolServer(host, port) }
    } else {
      logInfo(s"""Init ClientCache: locators=${locators.mkString(",")}, props=$gemFireProps""")
      locators.foreach { case (host, port)  => ccf.addPoolLocator(host, port) }
    }
    ccf
  }

  /** close the clientCache */
  override def close(): Unit =
    if (! clientCache.isClosed) clientCache.close()

  /** ----------------------------------------- */
  /** implementation of GeodeConnection trait */
  /** ----------------------------------------- */

  override def getQuery(queryString: String): Query =
    clientCache.asInstanceOf[RegionService].getQueryService.newQuery(queryString)

  override def validateRegion[K, V](regionPath: String): Unit = {
    val md = getRegionMetadata[K, V](regionPath)
    if (! md.isDefined) throw new RuntimeException(s"The region named $regionPath was not found")
  }

  def getRegionMetadata[K, V](regionPath: String): Option[RegionMetadata] = {
    import scala.collection.JavaConversions.setAsJavaSet
    val region = getRegionProxy[K, V](regionPath)
    val set0: JSet[Integer] = Set[Integer](0)
    val exec = FunctionService.onRegion(region).asInstanceOf[InternalExecution].withBucketFilter(set0)
    exec.setWaitOnExceptionFlag(true)
    try {
      val collector = exec.execute(RetrieveRegionMetadataFunction.ID)
      val r = collector.getResult.asInstanceOf[JList[RegionMetadata]]
      logDebug(r.get(0).toString)
      Some(r.get(0))
    } catch {
      case e: FunctionException => 
        if (e.getMessage.contains(s"The region named /$regionPath was not found")) None
        else throw e
    }
  }

  def getRegionProxy[K, V](regionPath: String): Region[K, V] = {
    val region1: Region[K, V] = clientCache.getRegion(regionPath).asInstanceOf[Region[K, V]]
    if (region1 != null) region1
    else DefaultGeodeConnection.regionLock.synchronized {
      val region2 = clientCache.getRegion(regionPath).asInstanceOf[Region[K, V]]
      if (region2 != null) region2
      else clientCache.createClientRegionFactory[K, V](ClientRegionShortcut.PROXY).create(regionPath)
    }
  }

  override def getRegionData[K, V](regionPath: String, whereClause: Option[String], split: GeodeRDDPartition): Iterator[(K, V)] = {
    val region = getRegionProxy[K, V](regionPath)
    val desc = s"""RDD($regionPath, "${whereClause.getOrElse("")}", ${split.index})"""
    val args : Array[String] = Array[String](whereClause.getOrElse(""), desc)
    val collector = new StructStreamingResultCollector(desc)
        // RetrieveRegionResultCollector[(K, V)]
    import scala.collection.JavaConversions.setAsJavaSet
    val exec = FunctionService.onRegion(region).withArgs(args).withCollector(collector).asInstanceOf[InternalExecution]
      .withBucketFilter(split.bucketSet.map(Integer.valueOf))
    exec.setWaitOnExceptionFlag(true)
    exec.execute(RetrieveRegionFunction.ID)
    collector.getResult.map{objs: Array[Object] => (objs(0).asInstanceOf[K], objs(1).asInstanceOf[V])}
  }

  override def executeQuery(regionPath: String, bucketSet: Set[Int], queryString: String) = {
    import scala.collection.JavaConversions.setAsJavaSet
    FunctionService.registerFunction(QueryFunction.getInstance())
    val collector = new QueryResultCollector
    val region = getRegionProxy(regionPath)
    val args: Array[String] = Array[String](queryString, bucketSet.toString)
    val exec = FunctionService.onRegion(region).withCollector(collector).asInstanceOf[InternalExecution]
      .withBucketFilter(bucketSet.map(Integer.valueOf))
      .withArgs(args)
    exec.execute(QueryFunction.ID)
    collector.getResult
  }
}

private[connector] object DefaultGeodeConnection {
  /** a lock object only used by getRegionProxy...() */
  private val regionLock = new Object
}

/** The purpose of this class is making unit test DefaultGeodeConnectionManager easier */
class DefaultGeodeConnectionFactory {

  def newConnection(locators: Seq[(String, Int)], gemFireProps: Map[String, String] = Map.empty) =
    new DefaultGeodeConnection(locators, gemFireProps)

}
