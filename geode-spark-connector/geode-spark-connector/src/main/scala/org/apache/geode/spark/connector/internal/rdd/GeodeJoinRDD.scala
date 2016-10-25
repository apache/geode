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
package org.apache.geode.spark.connector.internal.rdd

import org.apache.geode.cache.Region
import org.apache.geode.spark.connector.GeodeConnectionConf
import org.apache.spark.{TaskContext, Partition}
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._

/**
 * An `RDD[T, V]` that will represent the result of a join between `left` RDD[T]
 * and the specified Geode Region[K, V].
 */
class GeodeJoinRDD[T, K, V] private[connector]
  ( left: RDD[T],
    func: T => K,
    val regionPath: String,
    val connConf: GeodeConnectionConf
  ) extends RDD[(T, V)](left.context, left.dependencies) {

  /** validate region existence when GeodeRDD object is created */
  validate()

  /** Validate region, and make sure it exists. */
  private def validate(): Unit = connConf.getConnection.validateRegion[K, V](regionPath)

  override protected def getPartitions: Array[Partition] = left.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[(T, V)] = {
    val region = connConf.getConnection.getRegionProxy[K, V](regionPath)
    if (func == null) computeWithoutFunc(split, context, region)
    else computeWithFunc(split, context, region)
  }

  /** T is (K, V1) since there's no map function `func` */
  private def computeWithoutFunc(split: Partition, context: TaskContext, region: Region[K, V]): Iterator[(T, V)] = {
    val leftPairs = left.iterator(split, context).toList.asInstanceOf[List[(K, _)]]
    val leftKeys = leftPairs.map { case (k, v) => k}.toSet
    // Note: get all will return (key, null) for non-exist entry, so remove those entries
    val rightPairs = region.getAll(leftKeys).filter { case (k, v) => v != null}
    leftPairs.filter{case (k, v) => rightPairs.contains(k)}
             .map {case (k, v) => ((k, v).asInstanceOf[T], rightPairs.get(k).get)}.toIterator
  }
  
  private def computeWithFunc(split: Partition, context: TaskContext, region: Region[K, V]): Iterator[(T, V)] = {
    val leftPairs = left.iterator(split, context).toList.map(t => (t, func(t)))
    val leftKeys = leftPairs.map { case (t, k) => k}.toSet
    // Note: get all will return (key, null) for non-exist entry, so remove those entries
    val rightPairs = region.getAll(leftKeys).filter { case (k, v) => v != null}
    leftPairs.filter { case (t, k) => rightPairs.contains(k)}.map {case (t, k) => (t, rightPairs.get(k).get)}.toIterator
  }
}
