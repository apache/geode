package io.pivotal.gemfire.spark.connector.internal.rdd

import com.gemstone.gemfire.cache.Region
import io.pivotal.gemfire.spark.connector.GemFireConnectionConf
import org.apache.spark.{Logging, TaskContext}

import scala.collection.Iterator
import collection.JavaConversions._

/** This trait provide some common code for pair and non-pair RDD writer */
private[rdd] trait GemFireRDDWriterTraceUtils {
  
  def mapDump(map: Map[_, _], num: Int): String = {
    val firstNum = map.take(num + 1)
    if (firstNum.size > num) s"$firstNum ..." else s"$firstNum"    
  }  
}

/**
 * Writer object that provides write function that saves non-pair RDD partitions to GemFire.
 * Those functions will be executed on Spark executors.
 * @param regionPath the full path of the region where the data is written to
 */
class GemFireRDDWriter[T, K, V]
(regionPath: String, connConf: GemFireConnectionConf) extends Serializable with GemFireRDDWriterTraceUtils with Logging {

  def write(func: T => (K, V))(taskContext: TaskContext, data: Iterator[T]): Unit = {
    val region: Region[K, V] = connConf.getConnection.getRegionProxy[K, V](regionPath)
    // todo. optimize batch size of putAll
    val map: Map[K, V] = data.map(func).toMap
    region.putAll(map)
    logDebug(s"${map.size} entries are saved to region $regionPath")
    logTrace(mapDump(map, 10))
  }
}


/**
 * Writer object that provides write function that saves pair RDD partitions to GemFire.
 * Those functions will be executed on Spark executors.
 * @param regionPath the full path of the region where the data is written to
 */
class GemFirePairRDDWriter[K, V]
(regionPath: String, connConf: GemFireConnectionConf) extends Serializable with GemFireRDDWriterTraceUtils with Logging {

  def write(taskContext: TaskContext, data: Iterator[(K, V)]): Unit = {
    val region: Region[K, V] = connConf.getConnection.getRegionProxy[K, V](regionPath)
    // todo. optimize batch size of putAll
    val map: Map[K, V] = data.toMap
    region.putAll(map)
    logDebug(s"${map.size} entries are saved to region $regionPath")
    logTrace(mapDump(map, 10))
  }
}

