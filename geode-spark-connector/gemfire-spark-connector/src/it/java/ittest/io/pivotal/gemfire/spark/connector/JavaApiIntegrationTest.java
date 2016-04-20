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
package ittest.io.pivotal.gemfire.spark.connector;

import com.gemstone.gemfire.cache.Region;
import io.pivotal.gemfire.spark.connector.GemFireConnection;
import io.pivotal.gemfire.spark.connector.GemFireConnectionConf;
import io.pivotal.gemfire.spark.connector.GemFireConnectionConf$;
import io.pivotal.gemfire.spark.connector.internal.DefaultGemFireConnectionManager$;
import io.pivotal.gemfire.spark.connector.javaapi.GemFireJavaRegionRDD;
import ittest.io.pivotal.gemfire.spark.connector.testkit.GemFireCluster$;
import ittest.io.pivotal.gemfire.spark.connector.testkit.IOUtils;
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
import io.pivotal.gemfire.spark.connector.package$;
import scala.Tuple2;
import scala.Option;
import scala.Some;

import java.util.*;

import static io.pivotal.gemfire.spark.connector.javaapi.GemFireJavaUtil.RDDSaveBatchSizePropKey;
import static io.pivotal.gemfire.spark.connector.javaapi.GemFireJavaUtil.javaFunctions;
import static org.junit.Assert.*;

public class JavaApiIntegrationTest extends JUnitSuite {

  static JavaSparkContext jsc = null;
  static GemFireConnectionConf connConf = null;
  
  static int numServers = 2;
  static int numObjects = 1000;
  static String regionPath = "pr_str_int_region";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // start gemfire cluster, and spark context
    Properties settings = new Properties();
    settings.setProperty("cache-xml-file", "src/it/resources/test-retrieve-regions.xml");
    settings.setProperty("num-of-servers", Integer.toString(numServers));
    int locatorPort = GemFireCluster$.MODULE$.start(settings);

    // start spark context in local mode
    Properties props = new Properties();
    props.put("log4j.logger.org.apache.spark", "INFO");
    props.put("log4j.logger.io.pivotal.gemfire.spark.connector","DEBUG");
    IOUtils.configTestLog4j("ERROR", props);
    SparkConf conf = new SparkConf()
            .setAppName("RetrieveRegionIntegrationTest")
            .setMaster("local[2]")
            .set(package$.MODULE$.GemFireLocatorPropKey(), "localhost:"+ locatorPort);
    // sc = new SparkContext(conf);
    jsc = new JavaSparkContext(conf);
    connConf = GemFireConnectionConf.apply(jsc.getConf());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    // stop connection, spark context, and gemfire cluster
    DefaultGemFireConnectionManager$.MODULE$.closeConnection(GemFireConnectionConf$.MODULE$.apply(jsc.getConf()));
    jsc.stop();
    GemFireCluster$.MODULE$.stop();
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

    GemFireConnection conn = connConf.getConnection();
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
  //   JavaRDD.saveToGemfire 
  // --------------------------------------------------------------------------------------------

  static class IntToStrIntPairFunction implements PairFunction<Integer, String, Integer> {
    @Override public Tuple2<String, Integer> call(Integer x) throws Exception {
      return new Tuple2<>("k_" + x, x);
    }
  }

  @Test
  public void testRDDSaveToGemfireWithDefaultConnConfAndOpConf() throws Exception {
    verifyRDDSaveToGemfire(true, true);
  }

  @Test
  public void testRDDSaveToGemfireWithDefaultConnConf() throws Exception {
    verifyRDDSaveToGemfire(true, false);
  }
  
  @Test
  public void testRDDSaveToGemfireWithConnConfAndOpConf() throws Exception {
    verifyRDDSaveToGemfire(false, true);
  }

  @Test
  public void testRDDSaveToGemfireWithConnConf() throws Exception {
    verifyRDDSaveToGemfire(false, false);
  }
  
  public void verifyRDDSaveToGemfire(boolean useDefaultConnConf, boolean useOpConf) throws Exception {
    Region<String, Integer> region = prepareStrIntRegion(regionPath, 0, 0);  // remove all entries
    JavaRDD<Integer> rdd1 = prepareIntJavaRDD(0, numObjects);

    PairFunction<Integer, String, Integer> func = new IntToStrIntPairFunction();
    Properties opConf = new Properties();
    opConf.put(RDDSaveBatchSizePropKey, "200");

    if (useDefaultConnConf) {
      if (useOpConf)
        javaFunctions(rdd1).saveToGemfire(regionPath, func, opConf);
      else
        javaFunctions(rdd1).saveToGemfire(regionPath, func);
    } else {
      if (useOpConf)
        javaFunctions(rdd1).saveToGemfire(regionPath, func, connConf, opConf);
      else
        javaFunctions(rdd1).saveToGemfire(regionPath, func, connConf);
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
  //   JavaPairRDD.saveToGemfire
  // --------------------------------------------------------------------------------------------

  @Test
  public void testPairRDDSaveToGemfireWithDefaultConnConfAndOpConf() throws Exception {
    verifyPairRDDSaveToGemfire(true, true);
  }

  @Test
  public void testPairRDDSaveToGemfireWithDefaultConnConf() throws Exception {
    verifyPairRDDSaveToGemfire(true, false);
  }
  
  @Test
  public void testPairRDDSaveToGemfireWithConnConfAndOpConf() throws Exception {
    verifyPairRDDSaveToGemfire(false, true);
  }

  @Test
  public void testPairRDDSaveToGemfireWithConnConf() throws Exception {
    verifyPairRDDSaveToGemfire(false, false);
  }
  
  public void verifyPairRDDSaveToGemfire(boolean useDefaultConnConf, boolean useOpConf) throws Exception {
    Region<String, Integer> region = prepareStrIntRegion(regionPath, 0, 0);  // remove all entries
    JavaPairRDD<String, Integer> rdd1 = prepareStrIntJavaPairRDD(0, numObjects);
    Properties opConf = new Properties();
    opConf.put(RDDSaveBatchSizePropKey, "200");

    if (useDefaultConnConf) {
      if (useOpConf)
        javaFunctions(rdd1).saveToGemfire(regionPath, opConf);
      else
        javaFunctions(rdd1).saveToGemfire(regionPath);
    } else {
      if (useOpConf)
        javaFunctions(rdd1).saveToGemfire(regionPath, connConf, opConf);
      else
        javaFunctions(rdd1).saveToGemfire(regionPath, connConf);
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
  //   JavaSparkContext.gemfireRegion and where clause
  // --------------------------------------------------------------------------------------------

  @Test
  public void testJavaSparkContextGemfireRegion() throws Exception {
    prepareStrIntRegion(regionPath, 0, numObjects);  // remove all entries
    Properties emptyProps = new Properties();
    GemFireJavaRegionRDD<String, Integer> rdd1 = javaFunctions(jsc).gemfireRegion(regionPath);
    GemFireJavaRegionRDD<String, Integer> rdd2 = javaFunctions(jsc).gemfireRegion(regionPath, emptyProps);
    GemFireJavaRegionRDD<String, Integer> rdd3 = javaFunctions(jsc).gemfireRegion(regionPath, connConf);
    GemFireJavaRegionRDD<String, Integer> rdd4 = javaFunctions(jsc).gemfireRegion(regionPath, connConf, emptyProps);
    GemFireJavaRegionRDD<String, Integer> rdd5 = rdd1.where("value.intValue() < 50");

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
  //   JavaPairRDD.joinGemfireRegion
  // --------------------------------------------------------------------------------------------

  @Test
  public void testPairRDDJoinWithSameKeyType() throws Exception {
    prepareStrIntRegion(regionPath, 0, numObjects);
    JavaPairRDD<String, Integer> rdd1 = prepareStrIntJavaPairRDD(-5, 10);

    JavaPairRDD<Tuple2<String, Integer>, Integer> rdd2a = javaFunctions(rdd1).joinGemfireRegion(regionPath);
    JavaPairRDD<Tuple2<String, Integer>, Integer> rdd2b = javaFunctions(rdd1).joinGemfireRegion(regionPath, connConf);
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

    JavaPairRDD<Tuple2<Integer, Integer>, Integer> rdd2a = javaFunctions(rdd1).joinGemfireRegion(regionPath, func);
    JavaPairRDD<Tuple2<Integer, Integer>, Integer> rdd2b = javaFunctions(rdd1).joinGemfireRegion(regionPath, func, connConf);
    //System.out.println("=== Result RDD =======\n" + rdd2a.collect() + "\n=========================");

    HashMap<Tuple2<Integer, Integer>, Integer> expectedMap = new HashMap<>();
    for (int i = 0; i < 10; i ++) {
      expectedMap.put(new Tuple2<>(i, i * 2), i);
    }
    matchMapAndPairList(expectedMap, rdd2a.collect());
    matchMapAndPairList(expectedMap, rdd2b.collect());
  }

  // --------------------------------------------------------------------------------------------
  //   JavaPairRDD.outerJoinGemfireRegion
  // --------------------------------------------------------------------------------------------

  @Test
  public void testPairRDDOuterJoinWithSameKeyType() throws Exception {
    prepareStrIntRegion(regionPath, 0, numObjects);
    JavaPairRDD<String, Integer> rdd1 = prepareStrIntJavaPairRDD(-5, 10);

    JavaPairRDD<Tuple2<String, Integer>, Option<Integer>> rdd2a = javaFunctions(rdd1).outerJoinGemfireRegion(regionPath);
    JavaPairRDD<Tuple2<String, Integer>, Option<Integer>> rdd2b = javaFunctions(rdd1).outerJoinGemfireRegion(regionPath, connConf);
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

    JavaPairRDD<Tuple2<Integer, Integer>, Option<Integer>> rdd2a = javaFunctions(rdd1).outerJoinGemfireRegion(regionPath, func);
    JavaPairRDD<Tuple2<Integer, Integer>, Option<Integer>> rdd2b = javaFunctions(rdd1).outerJoinGemfireRegion(regionPath, func, connConf);
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
  //   JavaRDD.joinGemfireRegion
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
    JavaPairRDD<Integer, Integer> rdd2a = javaFunctions(rdd1).joinGemfireRegion(regionPath, func);
    JavaPairRDD<Integer, Integer> rdd2b = javaFunctions(rdd1).joinGemfireRegion(regionPath, func, connConf);
    //System.out.println("=== Result RDD =======\n" + rdd2a.collect() + "\n=========================");

    HashMap<Integer, Integer> expectedMap = new HashMap<>();
    for (int i = 0; i < 10; i ++) {
      expectedMap.put(i, i);
    }
    matchMapAndPairList(expectedMap, rdd2a.collect());
    matchMapAndPairList(expectedMap, rdd2b.collect());
  }

  // --------------------------------------------------------------------------------------------
  //   JavaRDD.outerJoinGemfireRegion
  // --------------------------------------------------------------------------------------------

  @Test
  public void testRDDOuterJoinWithSameKeyType() throws Exception {
    prepareStrIntRegion(regionPath, 0, numObjects);
    JavaRDD<Integer> rdd1 = prepareIntJavaRDD(-5, 10);

    Function<Integer, String> func = new IntToStrKeyFunction();
    JavaPairRDD<Integer, Option<Integer>> rdd2a = javaFunctions(rdd1).outerJoinGemfireRegion(regionPath, func);
    JavaPairRDD<Integer, Option<Integer>> rdd2b = javaFunctions(rdd1).outerJoinGemfireRegion(regionPath, func, connConf);
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
