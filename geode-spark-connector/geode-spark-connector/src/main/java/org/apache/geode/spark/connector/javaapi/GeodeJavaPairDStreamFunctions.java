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
import org.apache.geode.spark.connector.streaming.GeodePairDStreamFunctions;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import java.util.Properties;

import static org.apache.geode.spark.connector.javaapi.JavaAPIHelper.*;

/**
 * A Java API wrapper over {@link org.apache.spark.streaming.api.java.JavaPairDStream}
 * to provide Geode Spark Connector functionality.
 *
 * <p>To obtain an instance of this wrapper, use one of the factory methods in {@link
 * org.apache.geode.spark.connector.javaapi.GeodeJavaUtil} class.</p>
 */
public class GeodeJavaPairDStreamFunctions<K, V> {
  
  public final GeodePairDStreamFunctions<K, V> dsf;

  public GeodeJavaPairDStreamFunctions(JavaPairDStream<K, V> ds) {    
    this.dsf = new GeodePairDStreamFunctions<K, V>(ds.dstream());
  }

  /**
   * Save the JavaPairDStream to Geode key-value store.
   * @param regionPath the full path of region that the DStream is stored  
   * @param connConf the GeodeConnectionConf object that provides connection to Geode cluster
   * @param opConf the optional parameters for this operation
   */  
  public void saveToGeode(String regionPath, GeodeConnectionConf connConf, Properties opConf) {
    dsf.saveToGeode(regionPath, connConf, propertiesToScalaMap(opConf));
  }

  /**
   * Save the JavaPairDStream to Geode key-value store.
   * @param regionPath the full path of region that the DStream is stored  
   * @param connConf the GeodeConnectionConf object that provides connection to Geode cluster
   */
  public void saveToGeode(String regionPath, GeodeConnectionConf connConf) {
    dsf.saveToGeode(regionPath, connConf, emptyStrStrMap());
  }

  /**
   * Save the JavaPairDStream to Geode key-value store.
   * @param regionPath the full path of region that the DStream is stored
   * @param opConf the optional parameters for this operation
   */
  public void saveToGeode(String regionPath, Properties opConf) {
    dsf.saveToGeode(regionPath, dsf.defaultConnectionConf(), propertiesToScalaMap(opConf));
  }

  /**
   * Save the JavaPairDStream to Geode key-value store.
   * @param regionPath the full path of region that the DStream is stored
   */
  public void saveToGeode(String regionPath) {
    dsf.saveToGeode(regionPath, dsf.defaultConnectionConf(), emptyStrStrMap());
  }

}
