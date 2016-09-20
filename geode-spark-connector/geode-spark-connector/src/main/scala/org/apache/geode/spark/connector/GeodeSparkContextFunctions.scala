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

import org.apache.geode.spark.connector.internal.rdd.GeodeRegionRDD
import org.apache.spark.SparkContext

import scala.reflect.ClassTag

/** Provides Geode specific methods on `SparkContext` */
class GeodeSparkContextFunctions(@transient sc: SparkContext) extends Serializable {

  /**
   * Expose a Geode region as a GeodeRDD
   * @param regionPath the full path of the region
   * @param connConf the GeodeConnectionConf that can be used to access the region
   * @param opConf use this to specify preferred partitioner
   *        and its parameters. The implementation will use it if it's applicable
   */
  def geodeRegion[K: ClassTag, V: ClassTag] (
    regionPath: String, connConf: GeodeConnectionConf = GeodeConnectionConf(sc.getConf),
    opConf: Map[String, String] = Map.empty): GeodeRegionRDD[K, V] =
    GeodeRegionRDD[K, V](sc, regionPath, connConf, opConf)

}
