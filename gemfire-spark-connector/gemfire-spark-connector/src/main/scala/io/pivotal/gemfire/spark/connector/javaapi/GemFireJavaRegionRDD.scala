package io.pivotal.gemfire.spark.connector.javaapi

import io.pivotal.gemfire.spark.connector.internal.rdd.GemFireRegionRDD
import org.apache.spark.api.java.JavaPairRDD

class GemFireJavaRegionRDD[K, V](rdd: GemFireRegionRDD[K, V]) extends JavaPairRDD[K, V](rdd)(rdd.kClassTag, rdd.vClassTag) {
  
  def where(whereClause: String): GemFireJavaRegionRDD[K, V] = new GemFireJavaRegionRDD(rdd.where(whereClause))
  
}
