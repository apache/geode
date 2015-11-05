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

import io.pivotal.gemfire.spark.connector.GemFireConnectionConf;
import io.pivotal.gemfire.spark.connector.GemFirePairRDDFunctions;
import io.pivotal.gemfire.spark.connector.internal.rdd.GemFireJoinRDD;
import io.pivotal.gemfire.spark.connector.internal.rdd.GemFireOuterJoinRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import scala.Option;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.util.Properties;

import static io.pivotal.gemfire.spark.connector.javaapi.JavaAPIHelper.*;

/**
 * A Java API wrapper over {@link org.apache.spark.api.java.JavaPairRDD} to provide GemFire Spark
 * Connector functionality.
 *
 * <p>To obtain an instance of this wrapper, use one of the factory methods in {@link
 * io.pivotal.gemfire.spark.connector.javaapi.GemFireJavaUtil} class.</p>
 */
public class GemFireJavaPairRDDFunctions<K, V> {

  public final GemFirePairRDDFunctions<K, V> rddf;

  public GemFireJavaPairRDDFunctions(JavaPairRDD<K, V> rdd) {
    this.rddf = new GemFirePairRDDFunctions<K, V>(rdd.rdd());
  }

  /**
   * Save the pair RDD to GemFire key-value store.
   * @param regionPath the full path of region that the RDD is stored
   * @param connConf the GemFireConnectionConf object that provides connection to GemFire cluster
   * @param opConf the parameters for this operation
   */
  public void saveToGemfire(String regionPath, GemFireConnectionConf connConf, Properties opConf) {
    rddf.saveToGemfire(regionPath, connConf, propertiesToScalaMap(opConf));
  }

  /**
   * Save the pair RDD to GemFire key-value store.
   * @param regionPath the full path of region that the RDD is stored
   * @param opConf the parameters for this operation
   */
  public void saveToGemfire(String regionPath, Properties opConf) {
    rddf.saveToGemfire(regionPath, rddf.defaultConnectionConf(), propertiesToScalaMap(opConf));
  }

  /**
   * Save the pair RDD to GemFire key-value store.
   * @param regionPath the full path of region that the RDD is stored
   * @param connConf the GemFireConnectionConf object that provides connection to GemFire cluster
   */
  public void saveToGemfire(String regionPath, GemFireConnectionConf connConf) {
    rddf.saveToGemfire(regionPath, connConf, emptyStrStrMap());
  }

  /**
   * Save the pair RDD to GemFire key-value store with the default GemFireConnector.
   * @param regionPath the full path of region that the RDD is stored
   */
  public void saveToGemfire(String regionPath) {
    rddf.saveToGemfire(regionPath, rddf.defaultConnectionConf(), emptyStrStrMap());
  }

  /**
   * Return an JavaPairRDD containing all pairs of elements with matching keys in
   * this RDD&lt;K, V> and the GemFire `Region&lt;K, V2>`. Each pair of elements
   * will be returned as a ((k, v), v2) tuple, where (k, v) is in this RDD and
   * (k, v2) is in the GemFire region.
   *
   * @param regionPath the region path of the GemFire region
   * @param <V2> the value type of the GemFire region
   * @return JavaPairRDD&lt;&lt;K, V>, V2>
   */  
  public <V2> JavaPairRDD<Tuple2<K, V>, V2> joinGemfireRegion(String regionPath) {
    return joinGemfireRegion(regionPath, rddf.defaultConnectionConf());
  }

  /**
   * Return an JavaPairRDD containing all pairs of elements with matching keys in
   * this RDD&lt;K, V> and the GemFire `Region&lt;K, V2>`. Each pair of elements
   * will be returned as a ((k, v), v2) tuple, where (k, v) is in this RDD and
   * (k, v2) is in the GemFire region.
   *
   * @param regionPath the region path of the GemFire region
   * @param connConf the GemFireConnectionConf object that provides connection to GemFire cluster
   * @param <V2> the value type of the GemFire region
   * @return JavaPairRDD&lt;&lt;K, V>, V2>
   */
  public <V2> JavaPairRDD<Tuple2<K, V>, V2> joinGemfireRegion(
    String regionPath, GemFireConnectionConf connConf) {
    GemFireJoinRDD<Tuple2<K, V>, K, V2> rdd = rddf.joinGemfireRegion(regionPath, connConf);
    ClassTag<Tuple2<K, V>> kt = fakeClassTag();
    ClassTag<V2> vt = fakeClassTag();
    return new JavaPairRDD<>(rdd, kt, vt);
  }

  /**
   * Return an RDD containing all pairs of elements with matching keys in this
   * RDD&lt;K, V> and the GemFire `Region&lt;K2, V2>`. The join key from RDD
   * element is generated by `func(K, V) => K2`, and the key from the GemFire
   * region is just the key of the key/value pair.
   *
   * Each pair of elements of result RDD will be returned as a ((k, v), v2) tuple,
   * where (k, v) is in this RDD and (k2, v2) is in the GemFire region.
   *
   * @param regionPath the region path of the GemFire region
   * @param func the function that generates region key from RDD element (K, V)
   * @param <K2> the key type of the GemFire region
   * @param <V2> the value type of the GemFire region
   * @return JavaPairRDD&lt;Tuple2&lt;K, V>, V2>
   */
  public <K2, V2> JavaPairRDD<Tuple2<K, V>, V2> joinGemfireRegion(
    String regionPath, Function<Tuple2<K, V>, K2> func) {
    return joinGemfireRegion(regionPath, func, rddf.defaultConnectionConf());
  }

  /**
   * Return an RDD containing all pairs of elements with matching keys in this
   * RDD&lt;K, V> and the GemFire `Region&lt;K2, V2>`. The join key from RDD 
   * element is generated by `func(K, V) => K2`, and the key from the GemFire 
   * region is just the key of the key/value pair.
   *
   * Each pair of elements of result RDD will be returned as a ((k, v), v2) tuple,
   * where (k, v) is in this RDD and (k2, v2) is in the GemFire region.
   *
   * @param regionPath the region path of the GemFire region
   * @param func the function that generates region key from RDD element (K, V)
   * @param connConf the GemFireConnectionConf object that provides connection to GemFire cluster
   * @param <K2> the key type of the GemFire region
   * @param <V2> the value type of the GemFire region
   * @return JavaPairRDD&lt;Tuple2&lt;K, V>, V2>
   */  
  public <K2, V2> JavaPairRDD<Tuple2<K, V>, V2> joinGemfireRegion(
    String regionPath, Function<Tuple2<K, V>, K2> func, GemFireConnectionConf connConf) {
    GemFireJoinRDD<Tuple2<K, V>, K2, V2> rdd = rddf.joinGemfireRegion(regionPath, func, connConf);
    ClassTag<Tuple2<K, V>> kt = fakeClassTag();
    ClassTag<V2> vt = fakeClassTag();
    return new JavaPairRDD<>(rdd, kt, vt);
  }

  /**
   * Perform a left outer join of this RDD&lt;K, V> and the GemFire `Region&lt;K, V2>`.
   * For each element (k, v) in this RDD, the resulting RDD will either contain
   * all pairs ((k, v), Some(v2)) for v2 in the GemFire region, or the pair
   * ((k, v), None)) if no element in the GemFire region have key k.
   *
   * @param regionPath the region path of the GemFire region
   * @param <V2> the value type of the GemFire region
   * @return JavaPairRDD&lt;Tuple2&lt;K, V>, Option&lt;V>>
   */
  public <V2> JavaPairRDD<Tuple2<K, V>, Option<V2>> outerJoinGemfireRegion(String regionPath) {
    return outerJoinGemfireRegion(regionPath, rddf.defaultConnectionConf());
  }

  /**
   * Perform a left outer join of this RDD&lt;K, V> and the GemFire `Region&lt;K, V2>`.
   * For each element (k, v) in this RDD, the resulting RDD will either contain
   * all pairs ((k, v), Some(v2)) for v2 in the GemFire region, or the pair
   * ((k, v), None)) if no element in the GemFire region have key k.
   *
   * @param regionPath the region path of the GemFire region
   * @param connConf the GemFireConnectionConf object that provides connection to GemFire cluster
   * @param <V2> the value type of the GemFire region
   * @return JavaPairRDD&lt;Tuple2&lt;K, V>, Option&lt;V>>
   */  
  public <V2> JavaPairRDD<Tuple2<K, V>, Option<V2>> outerJoinGemfireRegion(
    String regionPath, GemFireConnectionConf connConf) {
    GemFireOuterJoinRDD<Tuple2<K, V>, K, V2> rdd = rddf.outerJoinGemfireRegion(regionPath, connConf);
    ClassTag<Tuple2<K, V>> kt = fakeClassTag();
    ClassTag<Option<V2>> vt = fakeClassTag();
    return new JavaPairRDD<>(rdd, kt, vt);
  }

  /**
   * Perform a left outer join of this RDD&lt;K, V> and the GemFire `Region&lt;K2, V2>`.
   * The join key from RDD element is generated by `func(K, V) => K2`, and the
   * key from region is just the key of the key/value pair.
   *
   * For each element (k, v) in `this` RDD, the resulting RDD will either contain
   * all pairs ((k, v), Some(v2)) for v2 in the GemFire region, or the pair
   * ((k, v), None)) if no element in the GemFire region have key `func(k, v)`.
   *
   * @param regionPath the region path of the GemFire region
   * @param func the function that generates region key from RDD element (K, V)
   * @param <K2> the key type of the GemFire region
   * @param <V2> the value type of the GemFire region
   * @return JavaPairRDD&lt;Tuple2&lt;K, V>, Option&lt;V>>
   */
  public <K2, V2> JavaPairRDD<Tuple2<K, V>, Option<V2>> outerJoinGemfireRegion(
    String regionPath, Function<Tuple2<K, V>, K2> func) {
    return outerJoinGemfireRegion(regionPath, func, rddf.defaultConnectionConf());
  }

  /**
   * Perform a left outer join of this RDD&lt;K, V> and the GemFire `Region&lt;K2, V2>`.
   * The join key from RDD element is generated by `func(K, V) => K2`, and the
   * key from region is just the key of the key/value pair.
   *
   * For each element (k, v) in `this` RDD, the resulting RDD will either contain
   * all pairs ((k, v), Some(v2)) for v2 in the GemFire region, or the pair
   * ((k, v), None)) if no element in the GemFire region have key `func(k, v)`.
   *
   * @param regionPath the region path of the GemFire region
   * @param func the function that generates region key from RDD element (K, V)
   * @param connConf the GemFireConnectionConf object that provides connection to GemFire cluster
   * @param <K2> the key type of the GemFire region
   * @param <V2> the value type of the GemFire region
   * @return JavaPairRDD&lt;Tuple2&lt;K, V>, Option&lt;V>>
   */
  public <K2, V2> JavaPairRDD<Tuple2<K, V>, Option<V2>> outerJoinGemfireRegion(
    String regionPath, Function<Tuple2<K, V>, K2> func, GemFireConnectionConf connConf) {
    GemFireOuterJoinRDD<Tuple2<K, V>, K2, V2> rdd = rddf.outerJoinGemfireRegion(regionPath, func, connConf);
    ClassTag<Tuple2<K, V>> kt = fakeClassTag();
    ClassTag<Option<V2>> vt = fakeClassTag();
    return new JavaPairRDD<>(rdd, kt, vt);
  }

}
