/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pivotal.gemfire.spark

import io.pivotal.gemfire.spark.connector.internal.rdd.{ServerSplitsPartitioner, OnePartitionPartitioner}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.reflect.ClassTag

/**
 * The root package of Gemfire connector for Apache Spark.
 * Provides handy implicit conversions that add gemfire-specific
 * methods to `SparkContext` and `RDD`.
 */
package object connector {

  /** constants */
  final val GemFireLocatorPropKey = "spark.gemfire.locators"
  // partitioner related keys and values
  final val PreferredPartitionerPropKey = "preferred.partitioner"
  final val NumberPartitionsPerServerPropKey = "number.partitions.per.server"
  final val OnePartitionPartitionerName = OnePartitionPartitioner.name
  final val ServerSplitsPartitionerName = ServerSplitsPartitioner.name

  final val RDDSaveBatchSizePropKey = "rdd.save.batch.size"
  final val RDDSaveBatchSizeDefault = 10000
  
  /** implicits */
  
  implicit def toSparkContextFunctions(sc: SparkContext): GemFireSparkContextFunctions =
    new GemFireSparkContextFunctions(sc)

  implicit def toSQLContextFunctions(sqlContext: SQLContext): GemFireSQLContextFunctions =
    new GemFireSQLContextFunctions(sqlContext)

  implicit def toGemfirePairRDDFunctions[K: ClassTag, V: ClassTag]
    (self: RDD[(K, V)]): GemFirePairRDDFunctions[K, V] = new GemFirePairRDDFunctions(self)

  implicit def toGemfireRDDFunctions[T: ClassTag]
    (self: RDD[T]): GemFireRDDFunctions[T] = new GemFireRDDFunctions(self)

  /** utility implicits */
  
  /** convert Map[String, String] to java.util.Properties */
  implicit def map2Properties(map: Map[String,String]): java.util.Properties =
    (new java.util.Properties /: map) {case (props, (k,v)) => props.put(k,v); props}

  /** internal util methods */
  
  private[connector] def getRddPartitionsInfo(rdd: RDD[_], sep: String = "\n  "): String =
    rdd.partitions.zipWithIndex.map{case (p,i) => s"$i: $p loc=${rdd.preferredLocations(p)}"}.mkString(sep)

}
