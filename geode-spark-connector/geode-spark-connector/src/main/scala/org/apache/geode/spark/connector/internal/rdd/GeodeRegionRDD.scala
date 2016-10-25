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

import scala.collection.Seq
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.{TaskContext, Partition, SparkContext}
import org.apache.geode.spark.connector.{GeodeConnectionConf, PreferredPartitionerPropKey}
import org.apache.geode.spark.connector.internal.rdd.GeodeRDDPartitioner._

/**
 * This class exposes Geode region as a RDD.
 * @param sc the Spark Context
 * @param regionPath the full path of the region
 * @param connConf the GeodeConnectionConf to access the region
 * @param opConf the parameters for this operation, such as preferred partitioner.
 */
class GeodeRegionRDD[K, V] private[connector]
  (@transient sc: SparkContext,
   val regionPath: String,
   val connConf: GeodeConnectionConf,
   val opConf: Map[String, String] = Map.empty,
   val whereClause: Option[String] = None 
  ) (implicit ctk: ClassTag[K], ctv: ClassTag[V])
  extends RDD[(K, V)](sc, Seq.empty) {

  /** validate region existence when GeodeRDD object is created */
  validate()

  /** Validate region, and make sure it exists. */
  private def validate(): Unit = connConf.getConnection.validateRegion[K, V](regionPath)

  def kClassTag = ctk
  
  def vClassTag = ctv

  /**
   * method `copy` is used by method `where` that creates new immutable
   * GeodeRDD instance based this instance.
   */
  private def copy(
    regionPath: String = regionPath,
    connConf: GeodeConnectionConf = connConf,
    opConf: Map[String, String] = opConf,
    whereClause: Option[String] = None
  ): GeodeRegionRDD[K, V] = {

    require(sc != null,
    """RDD transformation requires a non-null SparkContext. Unfortunately
      |SparkContext in this GeodeRDD is null. This can happen after 
      |GeodeRDD has been deserialized. SparkContext is not Serializable,
      |therefore it deserializes to null. RDD transformations are not allowed
      |inside lambdas used in other RDD transformations.""".stripMargin )

    new GeodeRegionRDD[K, V](sc, regionPath, connConf, opConf, whereClause)
  }

  /** When where clause is specified, OQL query
    * `select key, value from /<region-path>.entries where <where clause> `
    * is used to filter the dataset.
    */
  def where(whereClause: Option[String]): GeodeRegionRDD[K, V] = {
    if (whereClause.isDefined) copy(whereClause = whereClause)
    else this
  }

  /** this version is for Java API that doesn't use scala.Option */
  def where(whereClause: String): GeodeRegionRDD[K, V] = {
    if (whereClause == null || whereClause.trim.isEmpty) this
    else copy(whereClause = Option(whereClause.trim))
  }

  /**
   * Use preferred partitioner generate partitions. `defaultReplicatedRegionPartitioner`
   * will be used if it's a replicated region. 
   */
  override def getPartitions: Array[Partition] = {
    val conn = connConf.getConnection
    val md = conn.getRegionMetadata[K, V](regionPath)
    md match {
      case None => throw new RuntimeException(s"region $regionPath was not found.")
      case Some(data) =>
        logInfo(s"""RDD id=${this.id} region=$regionPath conn=${connConf.locators.mkString(",")}, env=$opConf""")
        val p = if (data.isPartitioned) preferredPartitioner else defaultReplicatedRegionPartitioner
        val splits = p.partitions[K, V](conn, data, opConf)
        logDebug(s"""RDD id=${this.id} region=$regionPath partitions=\n  ${splits.mkString("\n  ")}""")
        splits
    }
  }

  /**
   * provide preferred location(s) (host name(s)) of the given partition. 
   * Only some partitioner implementation(s) provides this info, which is
   * useful when Spark cluster and Geode cluster share some hosts.
   */
  override def getPreferredLocations(split: Partition) =
    split.asInstanceOf[GeodeRDDPartition].locations

  /**
   * Get preferred partitioner. return `defaultPartitionedRegionPartitioner` if none
   * preference is specified. 
   */
  private def preferredPartitioner = 
    GeodeRDDPartitioner(opConf.getOrElse(
      PreferredPartitionerPropKey, GeodeRDDPartitioner.defaultPartitionedRegionPartitioner.name))

  /** materialize a RDD partition */
  override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = {
    val partition = split.asInstanceOf[GeodeRDDPartition]
    logDebug(s"compute RDD id=${this.id} partition $partition")
    connConf.getConnection.getRegionData[K,V](regionPath, whereClause, partition)
    // new InterruptibleIterator(context, split.asInstanceOf[GeodeRDDPartition[K, V]].iterator)
  }
}

object GeodeRegionRDD {

  def apply[K: ClassTag, V: ClassTag](sc: SparkContext, regionPath: String,
    connConf: GeodeConnectionConf, opConf: Map[String, String] = Map.empty)
    : GeodeRegionRDD[K, V] =
    new GeodeRegionRDD[K, V](sc, regionPath, connConf, opConf)

}
