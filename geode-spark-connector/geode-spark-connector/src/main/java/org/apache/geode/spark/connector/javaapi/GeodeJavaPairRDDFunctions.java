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

import org.apache.geode.spark.connector.GeodeConnectionConf;
import org.apache.geode.spark.connector.GeodePairRDDFunctions;
import org.apache.geode.spark.connector.internal.rdd.GeodeJoinRDD;
import org.apache.geode.spark.connector.internal.rdd.GeodeOuterJoinRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import scala.Option;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.util.Properties;

import static org.apache.geode.spark.connector.javaapi.JavaAPIHelper.*;

/**
 * A Java API wrapper over {@link org.apache.spark.api.java.JavaPairRDD} to provide Geode Spark
 * Connector functionality.
 *
 * <p>To obtain an instance of this wrapper, use one of the factory methods in {@link
 * org.apache.geode.spark.connector.javaapi.GeodeJavaUtil} class.</p>
 */
public class GeodeJavaPairRDDFunctions<K, V> {

  public final GeodePairRDDFunctions<K, V> rddf;

  public GeodeJavaPairRDDFunctions(JavaPairRDD<K, V> rdd) {
    this.rddf = new GeodePairRDDFunctions<K, V>(rdd.rdd());
  }

  /**
   * Save the pair RDD to Geode key-value store.
   * @param regionPath the full path of region that the RDD is stored
   * @param connConf the GeodeConnectionConf object that provides connection to Geode cluster
   * @param opConf the parameters for this operation
   */
  public void saveToGeode(String regionPath, GeodeConnectionConf connConf, Properties opConf) {
    rddf.saveToGeode(regionPath, connConf, propertiesToScalaMap(opConf));
  }

  /**
   * Save the pair RDD to Geode key-value store.
   * @param regionPath the full path of region that the RDD is stored
   * @param opConf the parameters for this operation
   */
  public void saveToGeode(String regionPath, Properties opConf) {
    rddf.saveToGeode(regionPath, rddf.defaultConnectionConf(), propertiesToScalaMap(opConf));
  }

  /**
   * Save the pair RDD to Geode key-value store.
   * @param regionPath the full path of region that the RDD is stored
   * @param connConf the GeodeConnectionConf object that provides connection to Geode cluster
   */
  public void saveToGeode(String regionPath, GeodeConnectionConf connConf) {
    rddf.saveToGeode(regionPath, connConf, emptyStrStrMap());
  }

  /**
   * Save the pair RDD to Geode key-value store with the default GeodeConnector.
   * @param regionPath the full path of region that the RDD is stored
   */
  public void saveToGeode(String regionPath) {
    rddf.saveToGeode(regionPath, rddf.defaultConnectionConf(), emptyStrStrMap());
  }

  /**
   * Return an JavaPairRDD containing all pairs of elements with matching keys in
   * this RDD&lt;K, V> and the Geode `Region&lt;K, V2>`. Each pair of elements
   * will be returned as a ((k, v), v2) tuple, where (k, v) is in this RDD and
   * (k, v2) is in the Geode region.
   *
   * @param regionPath the region path of the Geode region
   * @param <V2> the value type of the Geode region
   * @return JavaPairRDD&lt;&lt;K, V>, V2>
   */  
  public <V2> JavaPairRDD<Tuple2<K, V>, V2> joinGeodeRegion(String regionPath) {
    return joinGeodeRegion(regionPath, rddf.defaultConnectionConf());
  }

  /**
   * Return an JavaPairRDD containing all pairs of elements with matching keys in
   * this RDD&lt;K, V> and the Geode `Region&lt;K, V2>`. Each pair of elements
   * will be returned as a ((k, v), v2) tuple, where (k, v) is in this RDD and
   * (k, v2) is in the Geode region.
   *
   * @param regionPath the region path of the Geode region
   * @param connConf the GeodeConnectionConf object that provides connection to Geode cluster
   * @param <V2> the value type of the Geode region
   * @return JavaPairRDD&lt;&lt;K, V>, V2>
   */
  public <V2> JavaPairRDD<Tuple2<K, V>, V2> joinGeodeRegion(
    String regionPath, GeodeConnectionConf connConf) {
    GeodeJoinRDD<Tuple2<K, V>, K, V2> rdd = rddf.joinGeodeRegion(regionPath, connConf);
    ClassTag<Tuple2<K, V>> kt = fakeClassTag();
    ClassTag<V2> vt = fakeClassTag();
    return new JavaPairRDD<>(rdd, kt, vt);
  }

  /**
   * Return an RDD containing all pairs of elements with matching keys in this
   * RDD&lt;K, V> and the Geode `Region&lt;K2, V2>`. The join key from RDD
   * element is generated by `func(K, V) => K2`, and the key from the Geode
   * region is just the key of the key/value pair.
   *
   * Each pair of elements of result RDD will be returned as a ((k, v), v2) tuple,
   * where (k, v) is in this RDD and (k2, v2) is in the Geode region.
   *
   * @param regionPath the region path of the Geode region
   * @param func the function that generates region key from RDD element (K, V)
   * @param <K2> the key type of the Geode region
   * @param <V2> the value type of the Geode region
   * @return JavaPairRDD&lt;Tuple2&lt;K, V>, V2>
   */
  public <K2, V2> JavaPairRDD<Tuple2<K, V>, V2> joinGeodeRegion(
    String regionPath, Function<Tuple2<K, V>, K2> func) {
    return joinGeodeRegion(regionPath, func, rddf.defaultConnectionConf());
  }

  /**
   * Return an RDD containing all pairs of elements with matching keys in this
   * RDD&lt;K, V> and the Geode `Region&lt;K2, V2>`. The join key from RDD 
   * element is generated by `func(K, V) => K2`, and the key from the Geode 
   * region is just the key of the key/value pair.
   *
   * Each pair of elements of result RDD will be returned as a ((k, v), v2) tuple,
   * where (k, v) is in this RDD and (k2, v2) is in the Geode region.
   *
   * @param regionPath the region path of the Geode region
   * @param func the function that generates region key from RDD element (K, V)
   * @param connConf the GeodeConnectionConf object that provides connection to Geode cluster
   * @param <K2> the key type of the Geode region
   * @param <V2> the value type of the Geode region
   * @return JavaPairRDD&lt;Tuple2&lt;K, V>, V2>
   */  
  public <K2, V2> JavaPairRDD<Tuple2<K, V>, V2> joinGeodeRegion(
    String regionPath, Function<Tuple2<K, V>, K2> func, GeodeConnectionConf connConf) {
    GeodeJoinRDD<Tuple2<K, V>, K2, V2> rdd = rddf.joinGeodeRegion(regionPath, func, connConf);
    ClassTag<Tuple2<K, V>> kt = fakeClassTag();
    ClassTag<V2> vt = fakeClassTag();
    return new JavaPairRDD<>(rdd, kt, vt);
  }

  /**
   * Perform a left outer join of this RDD&lt;K, V> and the Geode `Region&lt;K, V2>`.
   * For each element (k, v) in this RDD, the resulting RDD will either contain
   * all pairs ((k, v), Some(v2)) for v2 in the Geode region, or the pair
   * ((k, v), None)) if no element in the Geode region have key k.
   *
   * @param regionPath the region path of the Geode region
   * @param <V2> the value type of the Geode region
   * @return JavaPairRDD&lt;Tuple2&lt;K, V>, Option&lt;V>>
   */
  public <V2> JavaPairRDD<Tuple2<K, V>, Option<V2>> outerJoinGeodeRegion(String regionPath) {
    return outerJoinGeodeRegion(regionPath, rddf.defaultConnectionConf());
  }

  /**
   * Perform a left outer join of this RDD&lt;K, V> and the Geode `Region&lt;K, V2>`.
   * For each element (k, v) in this RDD, the resulting RDD will either contain
   * all pairs ((k, v), Some(v2)) for v2 in the Geode region, or the pair
   * ((k, v), None)) if no element in the Geode region have key k.
   *
   * @param regionPath the region path of the Geode region
   * @param connConf the GeodeConnectionConf object that provides connection to Geode cluster
   * @param <V2> the value type of the Geode region
   * @return JavaPairRDD&lt;Tuple2&lt;K, V>, Option&lt;V>>
   */  
  public <V2> JavaPairRDD<Tuple2<K, V>, Option<V2>> outerJoinGeodeRegion(
    String regionPath, GeodeConnectionConf connConf) {
    GeodeOuterJoinRDD<Tuple2<K, V>, K, V2> rdd = rddf.outerJoinGeodeRegion(regionPath, connConf);
    ClassTag<Tuple2<K, V>> kt = fakeClassTag();
    ClassTag<Option<V2>> vt = fakeClassTag();
    return new JavaPairRDD<>(rdd, kt, vt);
  }

  /**
   * Perform a left outer join of this RDD&lt;K, V> and the Geode `Region&lt;K2, V2>`.
   * The join key from RDD element is generated by `func(K, V) => K2`, and the
   * key from region is just the key of the key/value pair.
   *
   * For each element (k, v) in `this` RDD, the resulting RDD will either contain
   * all pairs ((k, v), Some(v2)) for v2 in the Geode region, or the pair
   * ((k, v), None)) if no element in the Geode region have key `func(k, v)`.
   *
   * @param regionPath the region path of the Geode region
   * @param func the function that generates region key from RDD element (K, V)
   * @param <K2> the key type of the Geode region
   * @param <V2> the value type of the Geode region
   * @return JavaPairRDD&lt;Tuple2&lt;K, V>, Option&lt;V>>
   */
  public <K2, V2> JavaPairRDD<Tuple2<K, V>, Option<V2>> outerJoinGeodeRegion(
    String regionPath, Function<Tuple2<K, V>, K2> func) {
    return outerJoinGeodeRegion(regionPath, func, rddf.defaultConnectionConf());
  }

  /**
   * Perform a left outer join of this RDD&lt;K, V> and the Geode `Region&lt;K2, V2>`.
   * The join key from RDD element is generated by `func(K, V) => K2`, and the
   * key from region is just the key of the key/value pair.
   *
   * For each element (k, v) in `this` RDD, the resulting RDD will either contain
   * all pairs ((k, v), Some(v2)) for v2 in the Geode region, or the pair
   * ((k, v), None)) if no element in the Geode region have key `func(k, v)`.
   *
   * @param regionPath the region path of the Geode region
   * @param func the function that generates region key from RDD element (K, V)
   * @param connConf the GeodeConnectionConf object that provides connection to Geode cluster
   * @param <K2> the key type of the Geode region
   * @param <V2> the value type of the Geode region
   * @return JavaPairRDD&lt;Tuple2&lt;K, V>, Option&lt;V>>
   */
  public <K2, V2> JavaPairRDD<Tuple2<K, V>, Option<V2>> outerJoinGeodeRegion(
    String regionPath, Function<Tuple2<K, V>, K2> func, GeodeConnectionConf connConf) {
    GeodeOuterJoinRDD<Tuple2<K, V>, K2, V2> rdd = rddf.outerJoinGeodeRegion(regionPath, func, connConf);
    ClassTag<Tuple2<K, V>> kt = fakeClassTag();
    ClassTag<Option<V2>> vt = fakeClassTag();
    return new JavaPairRDD<>(rdd, kt, vt);
  }

}
