package io.pivotal.gemfire.spark.connector

import io.pivotal.gemfire.spark.connector.internal.rdd.GemFireRegionRDD
import org.apache.spark.SparkContext

import scala.reflect.ClassTag

/** Provides GemFire specific methods on `SparkContext` */
class GemFireSparkContextFunctions(@transient sc: SparkContext) extends Serializable {

  /**
   * Expose a GemFire region as a GemFireRDD
   * @param regionPath the full path of the region
   * @param connConf the GemFireConnectionConf that can be used to access the region
   * @param opConf use this to specify preferred partitioner
   *        and its parameters. The implementation will use it if it's applicable
   */
  def gemfireRegion[K: ClassTag, V: ClassTag] (
    regionPath: String, connConf: GemFireConnectionConf = GemFireConnectionConf(sc.getConf),
    opConf: Map[String, String] = Map.empty): GemFireRegionRDD[K, V] =
    GemFireRegionRDD[K, V](sc, regionPath, connConf, opConf)

}
