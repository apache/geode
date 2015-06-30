package io.pivotal.gemfire.spark.connector.internal.rdd

import org.apache.spark.Partition

/**
 * This serializable class represents a GemFireRDD partition. Each partition is mapped
 * to one or more buckets of region. The GemFireRDD can materialize the data of the 
 * partition based on all information contained here.
 * @param partitionId partition id, a 0 based number.
 * @param bucketSet region bucket id set for this partition. Set.empty means whole
 *                  region (used for replicated region)
 * @param locations preferred location for this partition                  
 */
case class GemFireRDDPartition (
  partitionId: Int, bucketSet: Set[Int], locations: Seq[String] = Nil)
  extends Partition  {
  
  override def index: Int = partitionId

}
