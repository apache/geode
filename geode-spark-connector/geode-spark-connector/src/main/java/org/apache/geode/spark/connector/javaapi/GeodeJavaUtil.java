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
package org.apache.geode.spark.connector.javaapi;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

import org.apache.geode.spark.connector.package$;

/**
 * The main entry point to Spark Geode Connector Java API.
 *
 * There are several helpful static factory methods which build useful wrappers
 * around Spark Context, Streaming Context and RDD. There are also helper methods
 * to convert JavaRDD<Tuple2<K, V>> to JavaPairRDD<K, V>.
 */
public final class GeodeJavaUtil {
  
  /** constants */
  public static String GeodeLocatorPropKey = package$.MODULE$.GeodeLocatorPropKey();
  // partitioner related keys and values
  public static String PreferredPartitionerPropKey = package$.MODULE$.PreferredPartitionerPropKey();
  public static String NumberPartitionsPerServerPropKey = package$.MODULE$.NumberPartitionsPerServerPropKey();
  public static String OnePartitionPartitionerName = package$.MODULE$.OnePartitionPartitionerName();
  public static String ServerSplitsPartitionerName = package$.MODULE$.ServerSplitsPartitionerName();
  public static String RDDSaveBatchSizePropKey = package$.MODULE$.RDDSaveBatchSizePropKey();
  public static int RDDSaveBatchSizeDefault = package$.MODULE$.RDDSaveBatchSizeDefault();
  
  /** The private constructor is used prevents user from creating instance of this class. */
  private GeodeJavaUtil() { }

  /**
   * A static factory method to create a {@link GeodeJavaSparkContextFunctions} based
   * on an existing {@link SparkContext} instance.
   */
  public static GeodeJavaSparkContextFunctions javaFunctions(SparkContext sc) {
    return new GeodeJavaSparkContextFunctions(sc);
  }

  /**
   * A static factory method to create a {@link GeodeJavaSparkContextFunctions} based
   * on an existing {@link JavaSparkContext} instance.
   */
  public static GeodeJavaSparkContextFunctions javaFunctions(JavaSparkContext jsc) {
    return new GeodeJavaSparkContextFunctions(JavaSparkContext.toSparkContext(jsc));
  }

  /**
   * A static factory method to create a {@link GeodeJavaPairRDDFunctions} based on an
   * existing {@link org.apache.spark.api.java.JavaPairRDD} instance.
   */
  public static <K, V> GeodeJavaPairRDDFunctions<K, V> javaFunctions(JavaPairRDD<K, V> rdd) {
    return new GeodeJavaPairRDDFunctions<K, V>(rdd);
  }

  /**
   * A static factory method to create a {@link GeodeJavaRDDFunctions} based on an
   * existing {@link org.apache.spark.api.java.JavaRDD} instance.
   */
  public static <T> GeodeJavaRDDFunctions<T> javaFunctions(JavaRDD<T> rdd) {
    return new GeodeJavaRDDFunctions<T>(rdd);
  }

  /**
   * A static factory method to create a {@link GeodeJavaPairDStreamFunctions} based on an
   * existing {@link org.apache.spark.streaming.api.java.JavaPairDStream} instance.
   */
  public static <K, V> GeodeJavaPairDStreamFunctions<K, V> javaFunctions(JavaPairDStream<K, V> ds) {
    return new GeodeJavaPairDStreamFunctions<>(ds);
  }

  /**
   * A static factory method to create a {@link GeodeJavaDStreamFunctions} based on an
   * existing {@link org.apache.spark.streaming.api.java.JavaDStream} instance.
   */
  public static <T> GeodeJavaDStreamFunctions<T> javaFunctions(JavaDStream<T> ds) {
    return new GeodeJavaDStreamFunctions<>(ds);
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
   * A static factory method to create a {@link GeodeJavaSQLContextFunctions} based
   * on an existing {@link SQLContext} instance.
   */
  public static GeodeJavaSQLContextFunctions javaFunctions(SQLContext sqlContext) {
    return new GeodeJavaSQLContextFunctions(sqlContext);
  }

}
