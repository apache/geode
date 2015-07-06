package io.pivotal.gemfire.spark.connector.streaming

import io.pivotal.gemfire.spark.connector.GemFireConnectionConf
import io.pivotal.gemfire.spark.connector.internal.rdd.{GemFirePairRDDWriter, GemFireRDDWriter}
import org.apache.spark.Logging
import org.apache.spark.api.java.function.PairFunction
import org.apache.spark.streaming.dstream.DStream

/**
 * Extra gemFire functions on DStream of non-pair elements through an implicit conversion.
 * Import `io.pivotal.gemfire.spark.connector.streaming._` at the top of your program to
 * use these functions.
 */
class GemFireDStreamFunctions[T](val dstream: DStream[T]) extends Serializable with Logging {

  /**
   * Save the DStream of non-pair elements to GemFire key-value store.
   * @param regionPath the full path of region that the DStream is stored
   * @param func the function that converts elements of the DStream to key/value pairs
   * @param connConf the GemFireConnectionConf object that provides connection to GemFire cluster
   */
  def saveToGemfire[K, V](
    regionPath: String, func: T => (K, V), connConf: GemFireConnectionConf = defaultConnectionConf): Unit = {
    connConf.getConnection.validateRegion[K, V](regionPath)
    val writer = new GemFireRDDWriter[T, K, V](regionPath, connConf)
    logInfo(s"""Save DStream region=$regionPath conn=${connConf.locators.mkString(",")}""")
    dstream.foreachRDD(rdd => rdd.sparkContext.runJob(rdd, writer.write(func) _))
  }

  /** this version of saveToGemfire is just for Java API */
  def saveToGemfire[K, V](regionPath: String, func: PairFunction[T, K, V], connConf: GemFireConnectionConf): Unit = {
    saveToGemfire[K, V](regionPath, func.call _, connConf)
  }

  private[connector] def defaultConnectionConf: GemFireConnectionConf =
    GemFireConnectionConf(dstream.context.sparkContext.getConf)
}


/**
 * Extra gemFire functions on DStream of (key, value) pairs through an implicit conversion.
 * Import `io.pivotal.gemfire.spark.connector.streaming._` at the top of your program to
 * use these functions.
 */
class GemFirePairDStreamFunctions[K, V](val dstream: DStream[(K,V)]) extends Serializable with Logging {

  /**
   * Save the DStream of pairs to GemFire key-value store without any conversion
   * @param regionPath the full path of region that the DStream is stored
   * @param connConf the GemFireConnectionConf object that provides connection to GemFire cluster
   */
  def saveToGemfire(regionPath: String, connConf: GemFireConnectionConf = defaultConnectionConf): Unit = {
    connConf.getConnection.validateRegion[K, V](regionPath)
    val writer = new GemFirePairRDDWriter[K, V](regionPath, connConf)
    logInfo(s"""Save DStream region=$regionPath conn=${connConf.locators.mkString(",")}""")
    dstream.foreachRDD(rdd => rdd.sparkContext.runJob(rdd, writer.write _))
  }

  private[connector] def defaultConnectionConf: GemFireConnectionConf =
    GemFireConnectionConf(dstream.context.sparkContext.getConf)
}