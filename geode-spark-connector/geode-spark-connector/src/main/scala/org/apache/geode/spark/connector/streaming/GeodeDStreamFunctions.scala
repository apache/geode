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
package org.apache.geode.spark.connector.streaming

import org.apache.geode.spark.connector.GeodeConnectionConf
import org.apache.geode.spark.connector.internal.rdd.{GeodePairRDDWriter, GeodeRDDWriter}
import org.apache.spark.Logging
import org.apache.spark.api.java.function.PairFunction
import org.apache.spark.streaming.dstream.DStream

/**
 * Extra geode functions on DStream of non-pair elements through an implicit conversion.
 * Import `org.apache.geode.spark.connector.streaming._` at the top of your program to
 * use these functions.
 */
class GeodeDStreamFunctions[T](val dstream: DStream[T]) extends Serializable with Logging {

  /**
   * Save the DStream of non-pair elements to Geode key-value store.
   * @param regionPath the full path of region that the DStream is stored
   * @param func the function that converts elements of the DStream to key/value pairs
   * @param connConf the GeodeConnectionConf object that provides connection to Geode cluster
   * @param opConf the optional parameters for this operation
   */
  def saveToGeode[K, V](
      regionPath: String, 
      func: T => (K, V), 
      connConf: GeodeConnectionConf = defaultConnectionConf, 
      opConf: Map[String, String] = Map.empty): Unit = {
    connConf.getConnection.validateRegion[K, V](regionPath)
    val writer = new GeodeRDDWriter[T, K, V](regionPath, connConf, opConf)
    logInfo(s"""Save DStream region=$regionPath conn=${connConf.locators.mkString(",")}""")
    dstream.foreachRDD(rdd => rdd.sparkContext.runJob(rdd, writer.write(func) _))
  }

  /** this version of saveToGeode is just for Java API */
  def saveToGeode[K, V](
      regionPath: String,
      func: PairFunction[T, K, V],
      connConf: GeodeConnectionConf,
      opConf: Map[String, String] ): Unit = {
    saveToGeode[K, V](regionPath, func.call _, connConf, opConf)
  }

  private[connector] def defaultConnectionConf: GeodeConnectionConf =
    GeodeConnectionConf(dstream.context.sparkContext.getConf)
}


/**
 * Extra geode functions on DStream of (key, value) pairs through an implicit conversion.
 * Import `org.apache.geode.spark.connector.streaming._` at the top of your program to
 * use these functions.
 */
class GeodePairDStreamFunctions[K, V](val dstream: DStream[(K,V)]) extends Serializable with Logging {

  /**
   * Save the DStream of pairs to Geode key-value store without any conversion
   * @param regionPath the full path of region that the DStream is stored
   * @param connConf the GeodeConnectionConf object that provides connection to Geode cluster
   * @param opConf the optional parameters for this operation
   */
  def saveToGeode(
      regionPath: String, 
      connConf: GeodeConnectionConf = defaultConnectionConf, 
      opConf: Map[String, String] = Map.empty): Unit = {
    connConf.getConnection.validateRegion[K, V](regionPath)
    val writer = new GeodePairRDDWriter[K, V](regionPath, connConf, opConf)
    logInfo(s"""Save DStream region=$regionPath conn=${connConf.locators.mkString(",")}""")
    dstream.foreachRDD(rdd => rdd.sparkContext.runJob(rdd, writer.write _))
  }

  private[connector] def defaultConnectionConf: GeodeConnectionConf =
    GeodeConnectionConf(dstream.context.sparkContext.getConf)
}
