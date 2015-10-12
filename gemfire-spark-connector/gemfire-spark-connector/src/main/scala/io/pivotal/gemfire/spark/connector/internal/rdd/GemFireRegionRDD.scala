package io.pivotal.gemfire.spark.connector.internal.rdd

import scala.collection.Seq
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.{TaskContext, Partition, SparkContext}
import io.pivotal.gemfire.spark.connector.{GemFireConnectionConf, PreferredPartitionerPropKey}
import io.pivotal.gemfire.spark.connector.internal.rdd.GemFireRDDPartitioner._

/**
 * This class exposes GemFire region as a RDD.
 * @param sc the Spark Context
 * @param regionPath the full path of the region
 * @param connConf the GemFireConnectionConf to access the region
 * @param opConf the parameters for this operation, such as preferred partitioner.
 */
class GemFireRegionRDD[K, V] private[connector]
  (@transient sc: SparkContext,
   val regionPath: String,
   val connConf: GemFireConnectionConf,
   val opConf: Map[String, String] = Map.empty,
   val whereClause: Option[String] = None 
  ) (implicit ctk: ClassTag[K], ctv: ClassTag[V])
  extends RDD[(K, V)](sc, Seq.empty) {

  /** validate region existence when GemFireRDD object is created */
  validate()

  /** Validate region, and make sure it exists. */
  private def validate(): Unit = connConf.getConnection.validateRegion[K, V](regionPath)

  def kClassTag = ctk
  
  def vClassTag = ctv

  /**
   * method `copy` is used by method `where` that creates new immutable
   * GemFireRDD instance based this instance.
   */
  private def copy(
    regionPath: String = regionPath,
    connConf: GemFireConnectionConf = connConf,
    opConf: Map[String, String] = opConf,
    whereClause: Option[String] = None
  ): GemFireRegionRDD[K, V] = {

    require(sc != null,
    """RDD transformation requires a non-null SparkContext. Unfortunately
      |SparkContext in this GemFireRDD is null. This can happen after 
      |GemFireRDD has been deserialized. SparkContext is not Serializable,
      |therefore it deserializes to null. RDD transformations are not allowed
      |inside lambdas used in other RDD transformations.""".stripMargin )

    new GemFireRegionRDD[K, V](sc, regionPath, connConf, opConf, whereClause)
  }

  /** When where clause is specified, OQL query
    * `select key, value from /<region-path>.entries where <where clause> `
    * is used to filter the dataset.
    */
  def where(whereClause: Option[String]): GemFireRegionRDD[K, V] = {
    if (whereClause.isDefined) copy(whereClause = whereClause)
    else this
  }

  /** this version is for Java API that doesn't use scala.Option */
  def where(whereClause: String): GemFireRegionRDD[K, V] = {
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
   * useful when Spark cluster and GemFire cluster share some hosts.
   */
  override def getPreferredLocations(split: Partition) =
    split.asInstanceOf[GemFireRDDPartition].locations

  /**
   * Get preferred partitioner. return `defaultPartitionedRegionPartitioner` if none
   * preference is specified. 
   */
  private def preferredPartitioner = 
    GemFireRDDPartitioner(opConf.getOrElse(
      PreferredPartitionerPropKey, GemFireRDDPartitioner.defaultPartitionedRegionPartitioner.name))

  /** materialize a RDD partition */
  override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = {
    val partition = split.asInstanceOf[GemFireRDDPartition]
    logDebug(s"compute RDD id=${this.id} partition $partition")
    connConf.getConnection.getRegionData[K,V](regionPath, whereClause, partition)
    // new InterruptibleIterator(context, split.asInstanceOf[GemFireRDDPartition[K, V]].iterator)
  }
}

object GemFireRegionRDD {

  def apply[K: ClassTag, V: ClassTag](sc: SparkContext, regionPath: String,
    connConf: GemFireConnectionConf, opConf: Map[String, String] = Map.empty)
    : GemFireRegionRDD[K, V] =
    new GemFireRegionRDD[K, V](sc, regionPath, connConf, opConf)

}
