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
package ittest.org.apache.geode.spark.connector;

import org.apache.geode.cache.Region;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.spark.connector.GeodeConnection;
import org.apache.geode.spark.connector.GeodeConnectionConf;
import org.apache.geode.spark.connector.GeodeConnectionConf$;
import org.apache.geode.spark.connector.internal.DefaultGeodeConnectionManager$;
import org.apache.geode.spark.connector.javaapi.GeodeJavaRegionRDD;
import ittest.org.apache.geode.spark.connector.testkit.GeodeCluster$;
import ittest.org.apache.geode.spark.connector.testkit.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.scalatest.junit.JUnitSuite;
import org.apache.geode.spark.connector.package$;
import scala.Tuple2;
import scala.Option;
import scala.Some;
import java.util.*;

import static org.apache.geode.spark.connector.javaapi.GeodeJavaUtil.RDDSaveBatchSizePropKey;
import static org.apache.geode.spark.connector.javaapi.GeodeJavaUtil.javaFunctions;
import static org.junit.Assert.*;

public class JavaApiIntegrationTest extends JUnitSuite {

  static JavaSparkContext jsc = null;
  static GeodeConnectionConf connConf = null;
  
  static int numServers = 2;
  static int numObjects = 1000;
  static String regionPath = "pr_str_int_region";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // start geode cluster, and spark context
    Properties settings = new Properties();
    settings.setProperty(ConfigurationProperties.CACHE_XML_FILE, "src/it/resources/test-retrieve-regions.xml");
    settings.setProperty("num-of-servers", Integer.toString(numServers));
    int locatorPort = GeodeCluster$.MODULE$.start(settings);

    // start spark context in local mode
    Properties props = new Properties();
    props.put("log4j.logger.org.apache.spark", "INFO");
    props.put("log4j.logger.org.apache.geode.spark.connector","DEBUG");
    IOUtils.configTestLog4j("ERROR", props);
    SparkConf conf = new SparkConf()
            .setAppName("RetrieveRegionIntegrationTest")
            .setMaster("local[2]")
            .set(package$.MODULE$.GeodeLocatorPropKey(), "localhost:"+ locatorPort);
    // sc = new SparkContext(conf);
    jsc = new JavaSparkContext(conf);
    connConf = GeodeConnectionConf.apply(jsc.getConf());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    // stop connection, spark context, and geode cluster
    DefaultGeodeConnectionManager$.MODULE$.closeConnection(GeodeConnectionConf$.MODULE$.apply(jsc.getConf()));
    jsc.stop();
    GeodeCluster$.MODULE$.stop();
  }

  // --------------------------------------------------------------------------------------------
  //   utility methods
  // --------------------------------------------------------------------------------------------

  private <K,V> void matchMapAndPairList(Map<K,V> map, List<Tuple2<K,V>> list) {
    assertTrue("size mismatch \nmap: " + map.toString() + "\nlist: " + list.toString(), map.size() == list.size());
    for (Tuple2<K, V> p : list) {
      assertTrue("value mismatch: k=" + p._1() + " v1=" + p._2() + " v2=" + map.get(p._1()),
                 p._2().equals(map.get(p._1())));
    }
  }

  private Region<String, Integer> prepareStrIntRegion(String regionPath, int start, int stop) {
    HashMap<String, Integer> entriesMap = new HashMap<>();
    for (int i = start; i < stop; i ++) {
      entriesMap.put("k_" + i, i);
    }

    GeodeConnection conn = connConf.getConnection();
    Region<String, Integer> region = conn.getRegionProxy(regionPath);
    region.removeAll(region.keySetOnServer());
    region.putAll(entriesMap);
    return region;
  }

  private JavaPairRDD<String, Integer> prepareStrIntJavaPairRDD(int start, int stop) {
    List<Tuple2<String, Integer>> data = new ArrayList<>();
    for (int i = start; i < stop; i ++) {
      data.add(new Tuple2<>("k_" + i, i));
    }
    return jsc.parallelizePairs(data);
  }

  private JavaPairRDD<Integer, Integer> prepareIntIntJavaPairRDD(int start, int stop) {
    List<Tuple2<Integer, Integer>> data = new ArrayList<>();
    for (int i = start; i < stop; i ++) {
      data.add(new Tuple2<>(i, i * 2));
    }
    return jsc.parallelizePairs(data);
  }

  private JavaRDD<Integer> prepareIntJavaRDD(int start, int stop) {
    List<Integer> data = new ArrayList<>();
    for (int i = start; i < stop; i ++) {
      data.add(i);
    }
    return jsc.parallelize(data);
  }

  // --------------------------------------------------------------------------------------------
  //   JavaRDD.saveToGeode
  // --------------------------------------------------------------------------------------------

  static class IntToStrIntPairFunction implements PairFunction<Integer, String, Integer> {
    @Override public Tuple2<String, Integer> call(Integer x) throws Exception {
      return new Tuple2<>("k_" + x, x);
    }
  }

  @Test
  public void testRDDSaveToGeodeWithDefaultConnConfAndOpConf() throws Exception {
    verifyRDDSaveToGeode(true, true);
  }

  @Test
  public void testRDDSaveToGeodeWithDefaultConnConf() throws Exception {
    verifyRDDSaveToGeode(true, false);
  }
  
  @Test
  public void testRDDSaveToGeodeWithConnConfAndOpConf() throws Exception {
    verifyRDDSaveToGeode(false, true);
  }

  @Test
  public void testRDDSaveToGeodeWithConnConf() throws Exception {
    verifyRDDSaveToGeode(false, false);
  }
  
  public void verifyRDDSaveToGeode(boolean useDefaultConnConf, boolean useOpConf) throws Exception {
    Region<String, Integer> region = prepareStrIntRegion(regionPath, 0, 0);  // remove all entries
    JavaRDD<Integer> rdd1 = prepareIntJavaRDD(0, numObjects);

    PairFunction<Integer, String, Integer> func = new IntToStrIntPairFunction();
    Properties opConf = new Properties();
    opConf.put(RDDSaveBatchSizePropKey, "200");

    if (useDefaultConnConf) {
      if (useOpConf)
        javaFunctions(rdd1).saveToGeode(regionPath, func, opConf);
      else
        javaFunctions(rdd1).saveToGeode(regionPath, func);
    } else {
      if (useOpConf)
        javaFunctions(rdd1).saveToGeode(regionPath, func, connConf, opConf);
      else
        javaFunctions(rdd1).saveToGeode(regionPath, func, connConf);
    }
    
    Set<String> keys = region.keySetOnServer();
    Map<String, Integer> map = region.getAll(keys);

    List<Tuple2<String, Integer>> expectedList = new ArrayList<>();

    for (int i = 0; i < numObjects; i ++) {
      expectedList.add(new Tuple2<>("k_" + i, i));
    }
    matchMapAndPairList(map, expectedList);
  }

  // --------------------------------------------------------------------------------------------
  //   JavaPairRDD.saveToGeode
  // --------------------------------------------------------------------------------------------

  @Test
  public void testPairRDDSaveToGeodeWithDefaultConnConfAndOpConf() throws Exception {
    verifyPairRDDSaveToGeode(true, true);
  }

  @Test
  public void testPairRDDSaveToGeodeWithDefaultConnConf() throws Exception {
    verifyPairRDDSaveToGeode(true, false);
  }
  
  @Test
  public void testPairRDDSaveToGeodeWithConnConfAndOpConf() throws Exception {
    verifyPairRDDSaveToGeode(false, true);
  }

  @Test
  public void testPairRDDSaveToGeodeWithConnConf() throws Exception {
    verifyPairRDDSaveToGeode(false, false);
  }
  
  public void verifyPairRDDSaveToGeode(boolean useDefaultConnConf, boolean useOpConf) throws Exception {
    Region<String, Integer> region = prepareStrIntRegion(regionPath, 0, 0);  // remove all entries
    JavaPairRDD<String, Integer> rdd1 = prepareStrIntJavaPairRDD(0, numObjects);
    Properties opConf = new Properties();
    opConf.put(RDDSaveBatchSizePropKey, "200");

    if (useDefaultConnConf) {
      if (useOpConf)
        javaFunctions(rdd1).saveToGeode(regionPath, opConf);
      else
        javaFunctions(rdd1).saveToGeode(regionPath);
    } else {
      if (useOpConf)
        javaFunctions(rdd1).saveToGeode(regionPath, connConf, opConf);
      else
        javaFunctions(rdd1).saveToGeode(regionPath, connConf);
    }

    Set<String> keys = region.keySetOnServer();
    Map<String, Integer> map = region.getAll(keys);

    List<Tuple2<String, Integer>> expectedList = new ArrayList<>();
    for (int i = 0; i < numObjects; i ++) {
      expectedList.add(new Tuple2<>("k_" + i, i));
    }
    matchMapAndPairList(map, expectedList);
  }

  // --------------------------------------------------------------------------------------------
  //   JavaSparkContext.geodeRegion and where clause
  // --------------------------------------------------------------------------------------------

  @Test
  public void testJavaSparkContextGeodeRegion() throws Exception {
    prepareStrIntRegion(regionPath, 0, numObjects);  // remove all entries
    Properties emptyProps = new Properties();
    GeodeJavaRegionRDD<String, Integer> rdd1 = javaFunctions(jsc).geodeRegion(regionPath);
    GeodeJavaRegionRDD<String, Integer> rdd2 = javaFunctions(jsc).geodeRegion(regionPath, emptyProps);
    GeodeJavaRegionRDD<String, Integer> rdd3 = javaFunctions(jsc).geodeRegion(regionPath, connConf);
    GeodeJavaRegionRDD<String, Integer> rdd4 = javaFunctions(jsc).geodeRegion(regionPath, connConf, emptyProps);
    GeodeJavaRegionRDD<String, Integer> rdd5 = rdd1.where("value.intValue() < 50");

    HashMap<String, Integer> expectedMap = new HashMap<>();
    for (int i = 0; i < numObjects; i ++) {
      expectedMap.put("k_" + i, i);
    }

    matchMapAndPairList(expectedMap, rdd1.collect());
    matchMapAndPairList(expectedMap, rdd2.collect());
    matchMapAndPairList(expectedMap, rdd3.collect());
    matchMapAndPairList(expectedMap, rdd4.collect());

    HashMap<String, Integer> expectedMap2 = new HashMap<>();
    for (int i = 0; i < 50; i ++) {
      expectedMap2.put("k_" + i, i);
    }

    matchMapAndPairList(expectedMap2, rdd5.collect());
  }

  // --------------------------------------------------------------------------------------------
  //   JavaPairRDD.joinGeodeRegion
  // --------------------------------------------------------------------------------------------

  @Test
  public void testPairRDDJoinWithSameKeyType() throws Exception {
    prepareStrIntRegion(regionPath, 0, numObjects);
    JavaPairRDD<String, Integer> rdd1 = prepareStrIntJavaPairRDD(-5, 10);

    JavaPairRDD<Tuple2<String, Integer>, Integer> rdd2a = javaFunctions(rdd1).joinGeodeRegion(regionPath);
    JavaPairRDD<Tuple2<String, Integer>, Integer> rdd2b = javaFunctions(rdd1).joinGeodeRegion(regionPath, connConf);
    // System.out.println("=== Result RDD =======\n" + rdd2a.collect() + "\n=========================");

    HashMap<Tuple2<String, Integer>, Integer> expectedMap = new HashMap<>();
    for (int i = 0; i < 10; i ++) {
      expectedMap.put(new Tuple2<>("k_" + i, i), i);
    }
    matchMapAndPairList(expectedMap, rdd2a.collect());
    matchMapAndPairList(expectedMap, rdd2b.collect());
  }

  static class IntIntPairToStrKeyFunction implements Function<Tuple2<Integer, Integer>, String> {
    @Override public String call(Tuple2<Integer, Integer> pair) throws Exception {
      return "k_" + pair._1();
    }
  }

  @Test
  public void testPairRDDJoinWithDiffKeyType() throws Exception {
    prepareStrIntRegion(regionPath, 0, numObjects);
    JavaPairRDD<Integer, Integer> rdd1 = prepareIntIntJavaPairRDD(-5, 10);
    Function<Tuple2<Integer, Integer>, String> func = new IntIntPairToStrKeyFunction();

    JavaPairRDD<Tuple2<Integer, Integer>, Integer> rdd2a = javaFunctions(rdd1).joinGeodeRegion(regionPath, func);
    JavaPairRDD<Tuple2<Integer, Integer>, Integer> rdd2b = javaFunctions(rdd1).joinGeodeRegion(regionPath, func, connConf);
    //System.out.println("=== Result RDD =======\n" + rdd2a.collect() + "\n=========================");

    HashMap<Tuple2<Integer, Integer>, Integer> expectedMap = new HashMap<>();
    for (int i = 0; i < 10; i ++) {
      expectedMap.put(new Tuple2<>(i, i * 2), i);
    }
    matchMapAndPairList(expectedMap, rdd2a.collect());
    matchMapAndPairList(expectedMap, rdd2b.collect());
  }

  // --------------------------------------------------------------------------------------------
  //   JavaPairRDD.outerJoinGeodeRegion
  // --------------------------------------------------------------------------------------------

  @Test
  public void testPairRDDOuterJoinWithSameKeyType() throws Exception {
    prepareStrIntRegion(regionPath, 0, numObjects);
    JavaPairRDD<String, Integer> rdd1 = prepareStrIntJavaPairRDD(-5, 10);

    JavaPairRDD<Tuple2<String, Integer>, Option<Integer>> rdd2a = javaFunctions(rdd1).outerJoinGeodeRegion(regionPath);
    JavaPairRDD<Tuple2<String, Integer>, Option<Integer>> rdd2b = javaFunctions(rdd1).outerJoinGeodeRegion(regionPath, connConf);
    //System.out.println("=== Result RDD =======\n" + rdd2a.collect() + "\n=========================");

    HashMap<Tuple2<String, Integer>, Option<Integer>> expectedMap = new HashMap<>();
    for (int i = -5; i < 10; i ++) {
      if (i < 0)
        expectedMap.put(new Tuple2<>("k_" + i, i), Option.apply((Integer) null));
      else
        expectedMap.put(new Tuple2<>("k_" + i, i), Some.apply(i));
    }
    matchMapAndPairList(expectedMap, rdd2a.collect());
    matchMapAndPairList(expectedMap, rdd2b.collect());
  }

  @Test
  public void testPairRDDOuterJoinWithDiffKeyType() throws Exception {
    prepareStrIntRegion(regionPath, 0, numObjects);
    JavaPairRDD<Integer, Integer> rdd1 = prepareIntIntJavaPairRDD(-5, 10);
    Function<Tuple2<Integer, Integer>, String> func = new IntIntPairToStrKeyFunction();

    JavaPairRDD<Tuple2<Integer, Integer>, Option<Integer>> rdd2a = javaFunctions(rdd1).outerJoinGeodeRegion(regionPath, func);
    JavaPairRDD<Tuple2<Integer, Integer>, Option<Integer>> rdd2b = javaFunctions(rdd1).outerJoinGeodeRegion(regionPath, func, connConf);
    //System.out.println("=== Result RDD =======\n" + rdd2a.collect() + "\n=========================");

    HashMap<Tuple2<Integer, Integer>, Option<Integer>> expectedMap = new HashMap<>();
    for (int i = -5; i < 10; i ++) {
      if (i < 0)
        expectedMap.put(new Tuple2<>(i, i * 2), Option.apply((Integer) null));
      else
        expectedMap.put(new Tuple2<>(i, i * 2), Some.apply(i));
    }
    matchMapAndPairList(expectedMap, rdd2a.collect());
    matchMapAndPairList(expectedMap, rdd2b.collect());
  }

  // --------------------------------------------------------------------------------------------
  //   JavaRDD.joinGeodeRegion
  // --------------------------------------------------------------------------------------------

  static class IntToStrKeyFunction implements Function<Integer, String> {
    @Override public String call(Integer x) throws Exception {
      return "k_" + x;
    }
  }

  @Test
  public void testRDDJoinWithSameKeyType() throws Exception {
    prepareStrIntRegion(regionPath, 0, numObjects);
    JavaRDD<Integer> rdd1 = prepareIntJavaRDD(-5, 10);

    Function<Integer, String> func = new IntToStrKeyFunction();
    JavaPairRDD<Integer, Integer> rdd2a = javaFunctions(rdd1).joinGeodeRegion(regionPath, func);
    JavaPairRDD<Integer, Integer> rdd2b = javaFunctions(rdd1).joinGeodeRegion(regionPath, func, connConf);
    //System.out.println("=== Result RDD =======\n" + rdd2a.collect() + "\n=========================");

    HashMap<Integer, Integer> expectedMap = new HashMap<>();
    for (int i = 0; i < 10; i ++) {
      expectedMap.put(i, i);
    }
    matchMapAndPairList(expectedMap, rdd2a.collect());
    matchMapAndPairList(expectedMap, rdd2b.collect());
  }

  // --------------------------------------------------------------------------------------------
  //   JavaRDD.outerJoinGeodeRegion
  // --------------------------------------------------------------------------------------------

  @Test
  public void testRDDOuterJoinWithSameKeyType() throws Exception {
    prepareStrIntRegion(regionPath, 0, numObjects);
    JavaRDD<Integer> rdd1 = prepareIntJavaRDD(-5, 10);

    Function<Integer, String> func = new IntToStrKeyFunction();
    JavaPairRDD<Integer, Option<Integer>> rdd2a = javaFunctions(rdd1).outerJoinGeodeRegion(regionPath, func);
    JavaPairRDD<Integer, Option<Integer>> rdd2b = javaFunctions(rdd1).outerJoinGeodeRegion(regionPath, func, connConf);
    //System.out.println("=== Result RDD =======\n" + rdd2a.collect() + "\n=========================");

    HashMap<Integer, Option<Integer>> expectedMap = new HashMap<>();
    for (int i = -5; i < 10; i ++) {
      if (i < 0)
        expectedMap.put(i, Option.apply((Integer) null));
      else
        expectedMap.put(i, Some.apply(i));
    }
    matchMapAndPairList(expectedMap, rdd2a.collect());
    matchMapAndPairList(expectedMap, rdd2b.collect());
  }

}
