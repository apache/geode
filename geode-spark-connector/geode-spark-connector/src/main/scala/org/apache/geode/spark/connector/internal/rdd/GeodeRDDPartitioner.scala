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

import org.apache.geode.spark.connector.GeodeConnection
import org.apache.geode.spark.connector.internal.RegionMetadata
import org.apache.spark.{Logging, Partition}

import scala.reflect.ClassTag

/**
 * A GeodeRDD partitioner is used to partition the region into multiple RDD partitions.
 */
trait GeodeRDDPartitioner extends Serializable {

  def name: String
  
  /** the function that generates partitions */
  def partitions[K: ClassTag, V: ClassTag]
    (conn: GeodeConnection, md: RegionMetadata, env: Map[String, String]): Array[Partition]
}

object GeodeRDDPartitioner extends Logging {

  /** To add new partitioner, just add it to the following list */
  final val partitioners: Map[String, GeodeRDDPartitioner] =
    List(OnePartitionPartitioner, ServerSplitsPartitioner).map(e => (e.name, e)).toMap

  /**
   * Get a partitioner based on given name, a default partitioner will be returned if there's
   * no partitioner for the given name. 
   */
  def apply(name: String = defaultPartitionedRegionPartitioner.name): GeodeRDDPartitioner = {
    val p = partitioners.get(name)
    if (p.isDefined) p.get else {
      logWarning(s"Invalid preferred partitioner name $name.")
      defaultPartitionedRegionPartitioner
    }
  }

  val defaultReplicatedRegionPartitioner = OnePartitionPartitioner

  val defaultPartitionedRegionPartitioner = ServerSplitsPartitioner

}
