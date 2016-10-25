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
import org.apache.geode.spark.connector.streaming.GeodeDStreamFunctions;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import java.util.Properties;

import static org.apache.geode.spark.connector.javaapi.JavaAPIHelper.*;

/**
 * A Java API wrapper over {@link org.apache.spark.streaming.api.java.JavaDStream}
 * to provide Geode Spark Connector functionality.
 *
 * <p>To obtain an instance of this wrapper, use one of the factory methods in {@link
 * org.apache.geode.spark.connector.javaapi.GeodeJavaUtil} class.</p>
 */ 
public class GeodeJavaDStreamFunctions<T> {
  
  public final GeodeDStreamFunctions<T> dsf;

  public GeodeJavaDStreamFunctions(JavaDStream<T> ds) {
    this.dsf = new GeodeDStreamFunctions<T>(ds.dstream());
  }

  /**
   * Save the JavaDStream to Geode key-value store.
   * @param regionPath the full path of region that the DStream is stored  
   * @param func the PairFunction that converts elements of JavaDStream to key/value pairs
   * @param connConf the GeodeConnectionConf object that provides connection to Geode cluster
   * @param opConf the optional parameters for this operation
   */
  public <K, V> void saveToGeode(
    String regionPath, PairFunction<T, K, V> func, GeodeConnectionConf connConf, Properties opConf) {
    dsf.saveToGeode(regionPath, func, connConf, propertiesToScalaMap(opConf));
  }

  /**
   * Save the JavaDStream to Geode key-value store.
   * @param regionPath the full path of region that the DStream is stored  
   * @param func the PairFunction that converts elements of JavaDStream to key/value pairs
   * @param opConf the optional  parameters for this operation
   */
  public <K, V> void saveToGeode(
          String regionPath, PairFunction<T, K, V> func, Properties opConf) {
    dsf.saveToGeode(regionPath, func, dsf.defaultConnectionConf(), propertiesToScalaMap(opConf));
  }

  /**
   * Save the JavaDStream to Geode key-value store.
   * @param regionPath the full path of region that the DStream is stored
   * @param func the PairFunction that converts elements of JavaDStream to key/value pairs
   * @param connConf the GeodeConnectionConf object that provides connection to Geode cluster
   */
  public <K, V> void saveToGeode(
          String regionPath, PairFunction<T, K, V> func, GeodeConnectionConf connConf) {
    dsf.saveToGeode(regionPath, func, connConf, emptyStrStrMap());
  }

  /**
   * Save the JavaDStream to Geode key-value store.
   * @param regionPath the full path of region that the DStream is stored
   * @param func the PairFunction that converts elements of JavaDStream to key/value pairs
   */
  public <K, V> void saveToGeode(
          String regionPath, PairFunction<T, K, V> func) {
    dsf.saveToGeode(regionPath, func, dsf.defaultConnectionConf(), emptyStrStrMap());
  }

}
