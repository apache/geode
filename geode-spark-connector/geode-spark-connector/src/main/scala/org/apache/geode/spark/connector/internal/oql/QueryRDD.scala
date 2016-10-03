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
package org.apache.geode.spark.connector.internal.oql

import org.apache.geode.spark.connector.GeodeConnectionConf
import org.apache.geode.spark.connector.internal.rdd.{GeodeRDDPartition, ServerSplitsPartitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.{TaskContext, SparkContext, Partition}
import scala.reflect.ClassTag

/**
 * An RDD that provides the functionality that read the OQL query result
 *
 * @param sc The SparkContext this RDD is associated with
 * @param queryString The OQL query string
 * @param connConf The GeodeConnectionConf that provide the GeodeConnection
 */
class QueryRDD[T](@transient sc: SparkContext,
                  queryString: String,
                  connConf: GeodeConnectionConf)
                 (implicit ct: ClassTag[T])
  extends RDD[T](sc, Seq.empty) {

  override def getPartitions: Array[Partition] = {
    val conn = connConf.getConnection
    val regionPath = getRegionPathFromQuery(queryString)
    val md = conn.getRegionMetadata(regionPath)
    md match {
      case Some(metadata) =>
        if (metadata.isPartitioned) {
          val splits = ServerSplitsPartitioner.partitions(conn, metadata, Map.empty)
          logInfo(s"QueryRDD.getPartitions():isPartitioned=true, partitions=${splits.mkString(",")}")
          splits
        }
        else {
          logInfo(s"QueryRDD.getPartitions():isPartitioned=false")
          Array[Partition](new GeodeRDDPartition(0, Set.empty))

        }
      case None => throw new RuntimeException(s"Region $regionPath metadata was not found.")
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val buckets = split.asInstanceOf[GeodeRDDPartition].bucketSet
    val regionPath = getRegionPathFromQuery(queryString)
    val result = connConf.getConnection.executeQuery(regionPath, buckets, queryString)
    result match {
      case it: Iterator[T] =>
        logInfo(s"QueryRDD.compute():query=$queryString, partition=$split")
        it
      case _ =>
        throw new RuntimeException("Unexpected OQL result: " + result.toString)
    }
  }

  private def getRegionPathFromQuery(queryString: String): String = {
    val r = QueryParser.parseOQL(queryString).get
    r match {
      case r: String =>
        val start = r.indexOf("/") + 1
        var end = r.indexOf(")")
        if (r.indexOf(".") > 0) end = math.min(r.indexOf("."), end)
        if (r.indexOf(",") > 0) end = math.min(r.indexOf(","), end)
        val regionPath = r.substring(start, end)
        regionPath
    }
  }
}
