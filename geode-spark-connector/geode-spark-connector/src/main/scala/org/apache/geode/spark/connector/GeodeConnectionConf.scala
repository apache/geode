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
package org.apache.geode.spark.connector

import org.apache.spark.SparkConf
import org.apache.geode.spark.connector.internal.{DefaultGeodeConnectionManager, LocatorHelper}

/**
 * Stores configuration of a connection to Geode cluster. It is serializable and can
 * be safely sent over network.
 *
 * @param locators Geode locator host:port pairs, the default is (localhost,10334)
 * @param geodeProps The initial geode properties to be used.
 * @param connectionManager GeodeConnectionFactory instance
 */
class GeodeConnectionConf(
   val locators: Seq[(String, Int)], 
   val geodeProps: Map[String, String] = Map.empty,
   connectionManager: GeodeConnectionManager = new DefaultGeodeConnectionManager
  ) extends Serializable {

  /** require at least 1 pair of (host,port) */
  require(locators.nonEmpty)
  
  def getConnection: GeodeConnection = connectionManager.getConnection(this)
  
}

object GeodeConnectionConf {

  /**
   * create GeodeConnectionConf object based on locator string and optional GeodeConnectionFactory
   * @param locatorStr Geode cluster locator string
   * @param connectionManager GeodeConnection factory
   */
  def apply(locatorStr: String, geodeProps: Map[String, String] = Map.empty)
    (implicit connectionManager: GeodeConnectionManager = new DefaultGeodeConnectionManager): GeodeConnectionConf = {
    new GeodeConnectionConf(LocatorHelper.parseLocatorsString(locatorStr), geodeProps, connectionManager)
  }

  /**
   * create GeodeConnectionConf object based on SparkConf. Note that implicit can
   * be used to control what GeodeConnectionFactory instance to use if desired
   * @param conf a SparkConf instance 
   */
  def apply(conf: SparkConf): GeodeConnectionConf = {
    val locatorStr = conf.getOption(GeodeLocatorPropKey).getOrElse(
      throw new RuntimeException(s"SparkConf does not contain property $GeodeLocatorPropKey"))
    // SparkConf only holds properties whose key starts with "spark.", In order to
    // put geode properties in SparkConf, all geode properties are prefixes with
    // "spark.geode.". This prefix was removed before the properties were put in `geodeProp`
    val prefix = "spark.geode."
    val geodeProps = conf.getAll.filter {
        case (k, v) => k.startsWith(prefix) && k != GeodeLocatorPropKey
      }.map { case (k, v) => (k.substring(prefix.length), v) }.toMap
    apply(locatorStr, geodeProps)
  }

}
