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
package io.pivotal.gemfire.spark.connector

import org.apache.spark.SparkConf
import io.pivotal.gemfire.spark.connector.internal.{DefaultGemFireConnectionManager, LocatorHelper}

/**
 * Stores configuration of a connection to GemFire cluster. It is serializable and can
 * be safely sent over network.
 *
 * @param locators GemFire locator host:port pairs, the default is (localhost,10334)
 * @param gemfireProps The initial gemfire properties to be used.
 * @param connectionManager GemFireConnectionFactory instance
 */
class GemFireConnectionConf(
   val locators: Seq[(String, Int)], 
   val gemfireProps: Map[String, String] = Map.empty,
   connectionManager: GemFireConnectionManager = new DefaultGemFireConnectionManager
  ) extends Serializable {

  /** require at least 1 pair of (host,port) */
  require(locators.nonEmpty)
  
  def getConnection: GemFireConnection = connectionManager.getConnection(this)
  
}

object GemFireConnectionConf {

  /**
   * create GemFireConnectionConf object based on locator string and optional GemFireConnectionFactory
   * @param locatorStr GemFire cluster locator string
   * @param connectionManager GemFireConnection factory
   */
  def apply(locatorStr: String, gemfireProps: Map[String, String] = Map.empty)
    (implicit connectionManager: GemFireConnectionManager = new DefaultGemFireConnectionManager): GemFireConnectionConf = {
    new GemFireConnectionConf(LocatorHelper.parseLocatorsString(locatorStr), gemfireProps, connectionManager)
  }

  /**
   * create GemFireConnectionConf object based on SparkConf. Note that implicit can
   * be used to control what GemFireConnectionFactory instance to use if desired
   * @param conf a SparkConf instance 
   */
  def apply(conf: SparkConf): GemFireConnectionConf = {
    val locatorStr = conf.getOption(GemFireLocatorPropKey).getOrElse(
      throw new RuntimeException(s"SparkConf does not contain property $GemFireLocatorPropKey"))
    // SparkConf only holds properties whose key starts with "spark.", In order to
    // put gemfire properties in SparkConf, all gemfire properties are prefixes with
    // "spark.gemfire.". This prefix was removed before the properties were put in `gemfireProp`
    val prefix = "spark.gemfire."
    val gemfireProps = conf.getAll.filter {
        case (k, v) => k.startsWith(prefix) && k != GemFireLocatorPropKey
      }.map { case (k, v) => (k.substring(prefix.length), v) }.toMap
    apply(locatorStr, gemfireProps)
  }

}
