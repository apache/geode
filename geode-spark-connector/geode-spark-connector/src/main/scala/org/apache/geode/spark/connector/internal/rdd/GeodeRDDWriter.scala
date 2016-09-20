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
import org.apache.geode.spark.connector._
import org.apache.spark.{Logging, TaskContext}

import scala.collection.Iterator
import java.util.{HashMap => JMap}

/** This trait provide some common code for pair and non-pair RDD writer */
private[rdd] abstract class GeodeRDDWriterBase (opConf: Map[String, String]) extends Serializable {

  val batchSize = try { opConf.getOrElse(RDDSaveBatchSizePropKey, RDDSaveBatchSizeDefault.toString).toInt}
                  catch { case e: NumberFormatException => RDDSaveBatchSizeDefault }

  def mapDump(map: Map[_, _], num: Int): String = {
    val firstNum = map.take(num + 1)
    if (firstNum.size > num) s"$firstNum ..." else s"$firstNum"    
  }  
}

/**
 * Writer object that provides write function that saves non-pair RDD partitions to Geode.
 * Those functions will be executed on Spark executors.
 * @param regionPath the full path of the region where the data is written to
 */
class GeodeRDDWriter[T, K, V] 
  (regionPath: String, connConf: GeodeConnectionConf, opConf: Map[String, String] = Map.empty)
  extends GeodeRDDWriterBase(opConf) with Serializable with Logging {

  def write(func: T => (K, V))(taskContext: TaskContext, data: Iterator[T]): Unit = {
    val region: Region[K, V] = connConf.getConnection.getRegionProxy[K, V](regionPath)
    var count = 0
    val chunks = data.grouped(batchSize)
    chunks.foreach { chunk =>
      val map = chunk.foldLeft(new JMap[K, V]()){case (m, t) => val (k, v) = func(t); m.put(k, v); m}
      region.putAll(map)
      count += chunk.length
    }
    logDebug(s"$count entries (batch.size = $batchSize) are saved to region $regionPath")
  }
}


/**
 * Writer object that provides write function that saves pair RDD partitions to Geode.
 * Those functions will be executed on Spark executors.
 * @param regionPath the full path of the region where the data is written to
 */
class GeodePairRDDWriter[K, V]
  (regionPath: String, connConf: GeodeConnectionConf, opConf: Map[String, String] = Map.empty)
  extends GeodeRDDWriterBase(opConf) with Serializable with Logging {

  def write(taskContext: TaskContext, data: Iterator[(K, V)]): Unit = {
    val region: Region[K, V] = connConf.getConnection.getRegionProxy[K, V](regionPath)
    var count = 0
    val chunks = data.grouped(batchSize)
    chunks.foreach { chunk =>
      val map = chunk.foldLeft(new JMap[K, V]()){case (m, (k,v)) => m.put(k,v); m}
      region.putAll(map)
      count += chunk.length
    }
    logDebug(s"$count entries (batch.batch = $batchSize) are saved to region $regionPath")
  }
}

