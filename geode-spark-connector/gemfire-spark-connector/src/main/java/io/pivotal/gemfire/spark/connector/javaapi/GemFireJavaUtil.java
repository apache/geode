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
package io.pivotal.gemfire.spark.connector.javaapi;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

import io.pivotal.gemfire.spark.connector.package$;

/**
 * The main entry point to Spark GemFire Connector Java API.
 *
 * There are several helpful static factory methods which build useful wrappers
 * around Spark Context, Streaming Context and RDD. There are also helper methods
 * to convert JavaRDD<Tuple2<K, V>> to JavaPairRDD<K, V>.
 */
public final class GemFireJavaUtil {
  
  /** constants */
  public static String GemFireLocatorPropKey = package$.MODULE$.GemFireLocatorPropKey();
  // partitioner related keys and values
  public static String PreferredPartitionerPropKey = package$.MODULE$.PreferredPartitionerPropKey();
  public static String NumberPartitionsPerServerPropKey = package$.MODULE$.NumberPartitionsPerServerPropKey();
  public static String OnePartitionPartitionerName = package$.MODULE$.OnePartitionPartitionerName();
  public static String ServerSplitsPartitionerName = package$.MODULE$.ServerSplitsPartitionerName();
  public static String RDDSaveBatchSizePropKey = package$.MODULE$.RDDSaveBatchSizePropKey();
  public static int RDDSaveBatchSizeDefault = package$.MODULE$.RDDSaveBatchSizeDefault();
  
  /** The private constructor is used prevents user from creating instance of this class. */
  private GemFireJavaUtil() { }

  /**
   * A static factory method to create a {@link GemFireJavaSparkContextFunctions} based
   * on an existing {@link SparkContext} instance.
   */
  public static GemFireJavaSparkContextFunctions javaFunctions(SparkContext sc) {
    return new GemFireJavaSparkContextFunctions(sc);
  }

  /**
   * A static factory method to create a {@link GemFireJavaSparkContextFunctions} based
   * on an existing {@link JavaSparkContext} instance.
   */
  public static GemFireJavaSparkContextFunctions javaFunctions(JavaSparkContext jsc) {
    return new GemFireJavaSparkContextFunctions(JavaSparkContext.toSparkContext(jsc));
  }

  /**
   * A static factory method to create a {@link GemFireJavaPairRDDFunctions} based on an
   * existing {@link org.apache.spark.api.java.JavaPairRDD} instance.
   */
  public static <K, V> GemFireJavaPairRDDFunctions<K, V> javaFunctions(JavaPairRDD<K, V> rdd) {
    return new GemFireJavaPairRDDFunctions<K, V>(rdd);
  }

  /**
   * A static factory method to create a {@link GemFireJavaRDDFunctions} based on an
   * existing {@link org.apache.spark.api.java.JavaRDD} instance.
   */
  public static <T> GemFireJavaRDDFunctions<T> javaFunctions(JavaRDD<T> rdd) {
    return new GemFireJavaRDDFunctions<T>(rdd);
  }

  /**
   * A static factory method to create a {@link GemFireJavaPairDStreamFunctions} based on an
   * existing {@link org.apache.spark.streaming.api.java.JavaPairDStream} instance.
   */
  public static <K, V> GemFireJavaPairDStreamFunctions<K, V> javaFunctions(JavaPairDStream<K, V> ds) {
    return new GemFireJavaPairDStreamFunctions<>(ds);
  }

  /**
   * A static factory method to create a {@link GemFireJavaDStreamFunctions} based on an
   * existing {@link org.apache.spark.streaming.api.java.JavaDStream} instance.
   */
  public static <T> GemFireJavaDStreamFunctions<T> javaFunctions(JavaDStream<T> ds) {
    return new GemFireJavaDStreamFunctions<>(ds);
  }

  /** Convert an instance of {@link org.apache.spark.api.java.JavaRDD}&lt;&lt;Tuple2&lt;K, V&gt;&gt;
   * to a {@link org.apache.spark.api.java.JavaPairRDD}&lt;K, V&gt;.
   */
  public static <K, V> JavaPairRDD<K, V> toJavaPairRDD(JavaRDD<Tuple2<K, V>> rdd) {
    return JavaAPIHelper.toJavaPairRDD(rdd);
  }

  /** Convert an instance of {@link org.apache.spark.streaming.api.java.JavaDStream}&lt;&lt;Tuple2&lt;K, V&gt;&gt;
   * to a {@link org.apache.spark.streaming.api.java.JavaPairDStream}&lt;K, V&gt;.
   */
  public static <K, V> JavaPairDStream<K, V> toJavaPairDStream(JavaDStream<Tuple2<K, V>> ds) {
    return JavaAPIHelper.toJavaPairDStream(ds);
  }

  /**
   * A static factory method to create a {@link GemFireJavaSQLContextFunctions} based
   * on an existing {@link SQLContext} instance.
   */
  public static GemFireJavaSQLContextFunctions javaFunctions(SQLContext sqlContext) {
    return new GemFireJavaSQLContextFunctions(sqlContext);
  }

}
