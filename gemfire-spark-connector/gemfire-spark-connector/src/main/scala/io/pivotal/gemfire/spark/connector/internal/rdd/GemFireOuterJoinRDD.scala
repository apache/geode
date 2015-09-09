package io.pivotal.gemfire.spark.connector.internal.rdd

import com.gemstone.gemfire.cache.Region
import io.pivotal.gemfire.spark.connector.GemFireConnectionConf
import org.apache.spark.{TaskContext, Partition}
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._

/**
 * An `RDD[ T, Option[V] ]` that represents the result of a left outer join 
 * between `left` RDD[T] and the specified GemFire Region[K, V].
 */
class GemFireOuterJoinRDD[T, K, V] private[connector]
 ( left: RDD[T],
   func: T => K,
   val regionPath: String,
   val connConf: GemFireConnectionConf
  ) extends RDD[(T, Option[V])](left.context, left.dependencies) {

  /** validate region existence when GemFireRDD object is created */
  validate()

  /** Validate region, and make sure it exists. */
  private def validate(): Unit = connConf.getConnection.validateRegion[K, V](regionPath)

  override protected def getPartitions: Array[Partition] = left.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[(T, Option[V])] = {
    val region = connConf.getConnection.getRegionProxy[K, V](regionPath)
    if (func == null) computeWithoutFunc(split, context, region)
    else computeWithFunc(split, context, region)
  }

  /** T is (K1, V1), and K1 and K are the same type since there's no map function `func` */
  private def computeWithoutFunc(split: Partition, context: TaskContext, region: Region[K, V]): Iterator[(T, Option[V])] = {
    val leftPairs = left.iterator(split, context).toList.asInstanceOf[List[(K, _)]]
    val leftKeys = leftPairs.map { case (k, v) => k}.toSet
    // Note: get all will return (key, null) for non-exist entry
    val rightPairs = region.getAll(leftKeys)
    // rightPairs is a java.util.Map, not scala map, so need to convert map.get() to Option
    leftPairs.map{ case (k, v) => ((k, v).asInstanceOf[T], Option(rightPairs.get(k))) }.toIterator
  }

  private def computeWithFunc(split: Partition, context: TaskContext, region: Region[K, V]): Iterator[(T, Option[V])] = {
    val leftPairs = left.iterator(split, context).toList.map(t => (t, func(t)))
    val leftKeys = leftPairs.map { case (t, k) => k}.toSet
    // Note: get all will return (key, null) for non-exist entry
    val rightPairs = region.getAll(leftKeys)
    // rightPairs is a java.util.Map, not scala map, so need to convert map.get() to Option
    leftPairs.map{ case (t, k) => (t, Option(rightPairs.get(k)))}.toIterator
  }
}

