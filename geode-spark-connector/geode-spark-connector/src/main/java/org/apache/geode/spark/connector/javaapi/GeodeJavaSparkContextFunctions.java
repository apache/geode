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
import org.apache.geode.spark.connector.internal.rdd.GeodeRegionRDD;
import org.apache.geode.spark.connector.internal.rdd.GeodeRegionRDD$;
import org.apache.spark.SparkContext;
import static org.apache.geode.spark.connector.javaapi.JavaAPIHelper.*;

import scala.reflect.ClassTag;
import java.util.Properties;

/**
 * Java API wrapper over {@link org.apache.spark.SparkContext} to provide Geode
 * Connector functionality.
 *
 * <p></p>To obtain an instance of this wrapper, use one of the factory methods in {@link
 * org.apache.geode.spark.connector.javaapi.GeodeJavaUtil} class.</p>
 */
public class GeodeJavaSparkContextFunctions {

  public final SparkContext sc;

  public GeodeJavaSparkContextFunctions(SparkContext sc) {
    this.sc = sc;
  }

  /**
   * Expose a Geode region as a JavaPairRDD
   * @param regionPath the full path of the region
   * @param connConf the GeodeConnectionConf that can be used to access the region
   * @param opConf the parameters for this operation, such as preferred partitioner.
   */
  public <K, V> GeodeJavaRegionRDD<K, V> geodeRegion(
    String regionPath, GeodeConnectionConf connConf, Properties opConf) {
    ClassTag<K> kt = fakeClassTag();
    ClassTag<V> vt = fakeClassTag();    
    GeodeRegionRDD<K, V>  rdd =  GeodeRegionRDD$.MODULE$.apply(
      sc, regionPath, connConf, propertiesToScalaMap(opConf), kt, vt);
    return new GeodeJavaRegionRDD<>(rdd);
  }

  /**
   * Expose a Geode region as a JavaPairRDD with default GeodeConnector and no preferred partitioner.
   * @param regionPath the full path of the region
   */
  public <K, V> GeodeJavaRegionRDD<K, V> geodeRegion(String regionPath) {
    GeodeConnectionConf connConf = GeodeConnectionConf.apply(sc.getConf());
    return geodeRegion(regionPath, connConf, new Properties());
  }

  /**
   * Expose a Geode region as a JavaPairRDD with no preferred partitioner.
   * @param regionPath the full path of the region
   * @param connConf the GeodeConnectionConf that can be used to access the region
   */
  public <K, V> GeodeJavaRegionRDD<K, V> geodeRegion(String regionPath, GeodeConnectionConf connConf) {
    return geodeRegion(regionPath, connConf, new Properties());
  }

  /**
   * Expose a Geode region as a JavaPairRDD with default GeodeConnector.
   * @param regionPath the full path of the region
   * @param opConf the parameters for this operation, such as preferred partitioner.
   */
  public <K, V> GeodeJavaRegionRDD<K, V> geodeRegion(String regionPath, Properties opConf) {
    GeodeConnectionConf connConf = GeodeConnectionConf.apply(sc.getConf());
    return geodeRegion(regionPath, connConf, opConf);
  }

}
