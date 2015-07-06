package io.pivotal.gemfire.spark.connector

import org.apache.spark.streaming.dstream.DStream

/**
 * Provides handy implicit conversions that add gemfire-specific methods to `DStream`.
 */
package object streaming {

  implicit def toGemFireDStreamFunctions[T](ds: DStream[T]): GemFireDStreamFunctions[T] =
    new GemFireDStreamFunctions[T](ds)

  implicit def toGemFirePairDStreamFunctions[K, V](ds: DStream[(K, V)]): GemFirePairDStreamFunctions[K, V] =
    new GemFirePairDStreamFunctions[K, V](ds)
  
}
