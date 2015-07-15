package io.pivotal.gemfire.spark.connector.internal.rdd

import com.gemstone.gemfire.cache.Region
import io.pivotal.gemfire.spark.connector._
import org.apache.spark.{Logging, TaskContext}

import scala.collection.Iterator
import java.util.{HashMap => JMap}

/** This trait provide some common code for pair and non-pair RDD writer */
private[rdd] abstract class GemFireRDDWriterBase (opConf: Map[String, String]) extends Serializable {

  val batchSize = try { opConf.getOrElse(RDDSaveBatchSizePropKey, RDDSaveBatchSizeDefault.toString).toInt}
                  catch { case e: NumberFormatException => RDDSaveBatchSizeDefault }

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
  (regionPath: String, connConf: GemFireConnectionConf, opConf: Map[String, String] = Map.empty)
  extends GemFireRDDWriterBase(opConf) with Serializable with Logging {

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
 * Writer object that provides write function that saves pair RDD partitions to GemFire.
 * Those functions will be executed on Spark executors.
 * @param regionPath the full path of the region where the data is written to
 */
class GemFirePairRDDWriter[K, V]
  (regionPath: String, connConf: GemFireConnectionConf, opConf: Map[String, String] = Map.empty)
  extends GemFireRDDWriterBase(opConf) with Serializable with Logging {

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

