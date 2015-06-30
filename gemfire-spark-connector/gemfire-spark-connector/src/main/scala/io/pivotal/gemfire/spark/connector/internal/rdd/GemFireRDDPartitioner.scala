package io.pivotal.gemfire.spark.connector.internal.rdd

import io.pivotal.gemfire.spark.connector.GemFireConnection
import io.pivotal.gemfire.spark.connector.internal.RegionMetadata
import org.apache.spark.{Logging, Partition}

import scala.reflect.ClassTag

/**
 * A GemFireRDD partitioner is used to partition the region into multiple RDD partitions.
 */
trait GemFireRDDPartitioner extends Serializable {

  def name: String
  
  /** the function that generates partitions */
  def partitions[K: ClassTag, V: ClassTag]
    (conn: GemFireConnection, md: RegionMetadata, env: Map[String, String]): Array[Partition]
}

object GemFireRDDPartitioner extends Logging {

  /** To add new partitioner, just add it to the following list */
  final val partitioners: Map[String, GemFireRDDPartitioner] =
    List(OnePartitionPartitioner, ServerSplitsPartitioner).map(e => (e.name, e)).toMap

  /**
   * Get a partitioner based on given name, a default partitioner will be returned if there's
   * no partitioner for the given name. 
   */
  def apply(name: String = defaultPartitionedRegionPartitioner.name): GemFireRDDPartitioner = {
    val p = partitioners.get(name)
    if (p.isDefined) p.get else {
      logWarning(s"Invalid preferred partitioner name $name.")
      defaultPartitionedRegionPartitioner
    }
  }

  val defaultReplicatedRegionPartitioner = OnePartitionPartitioner

  val defaultPartitionedRegionPartitioner = ServerSplitsPartitioner

}
