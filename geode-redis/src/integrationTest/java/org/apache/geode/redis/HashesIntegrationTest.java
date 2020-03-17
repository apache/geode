/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.redis;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanResult;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.junit.categories.RedisTest;

@Category({RedisTest.class})
public class HashesIntegrationTest {
  static Random rand;
  static Jedis jedis;
  static Jedis jedis2;
  private static GeodeRedisServer server;
  private static GemFireCache cache;
  private static int port = 6379;
  private static int ITERATION_COUNT = 4000;

  @BeforeClass
  public static void setUp() throws IOException {
    rand = new Random();
    CacheFactory cf = new CacheFactory();
    // cf.set("log-file", "redis.log");
    cf.set(LOG_LEVEL, "error");
    cf.set(MCAST_PORT, "0");
    cf.set(LOCATORS, "");
    cache = cf.create();
    port = AvailablePortHelper.getRandomAvailableTCPPort();

    System.setProperty(GeodeRedisServer.DEFAULT_REGION_SYS_PROP_NAME, "REPLICATE");

    server = new GeodeRedisServer("localhost", port);

    server.start();
    jedis = new Jedis("localhost", port, 10000000);
    jedis2 = new Jedis("localhost", port, 10000000);
  }

  @Test
  public void testHMSetHSetHLen() {
    int num = 10;
    String key = randString();
    Map<String, String> hash = new HashMap<String, String>();
    for (int i = 0; i < num; i++) {
      hash.put(randString(), randString());
    }
    String response = jedis.hmset(key, hash);
    assertTrue(response.equals("OK"));
    assertEquals(new Long(hash.size()), jedis.hlen(key));

    key = randString();
    hash = new HashMap<String, String>();
    for (int i = 0; i < num; i++) {
      hash.put(randString(), randString());
    }
    Set<String> keys = hash.keySet();
    Long count = 1L;
    for (String field : keys) {
      Long res = jedis.hset(key, field, hash.get(field));
      assertTrue(res == 1L);
      assertEquals(count++, jedis.hlen(key));
    }
  }

  @Test
  public void testHMGetHDelHGetAllHVals() {
    String key = randString();
    Map<String, String> hash = new HashMap<String, String>();
    for (int i = 0; i < 10; i++) {
      String m = randString();
      String f = randString();
      hash.put(m, f);
    }
    jedis.hmset(key, hash);
    Set<String> keys = hash.keySet();
    String[] keyArray = keys.toArray(new String[keys.size()]);
    List<String> retList = jedis.hmget(key, keyArray);

    for (int i = 0; i < keys.size(); i++) {
      assertEquals(retList.get(i), hash.get(keyArray[i]));
    }

    Map<String, String> retMap = jedis.hgetAll(key);

    assertEquals(retMap, hash);

    List<String> retVals = jedis.hvals(key);
    Set<String> retSet = new HashSet<String>(retVals);

    assertTrue(retSet.containsAll(hash.values()));

    jedis.hdel(key, keyArray);
    assertTrue(jedis.hlen(key) == 0);
  }


  @Test
  public void testHkeys() {
    String key = randString();
    Map<String, String> hash = new HashMap<String, String>();
    for (int i = 0; i < 10; i++) {
      hash.put(randString(), randString());
    }
    jedis.hmset(key, hash);

    Set<String> keys = hash.keySet();
    Set<String> retSet = jedis.hkeys(key);

    assertTrue(retSet.containsAll(keys));
  }

  @Test
  public void testHIncrBy() {
    String key = randString();
    String field = randString();

    Long incr = (long) rand.nextInt(50);
    if (incr == 0) {
      incr++;
    }

    long response1 = jedis.hincrBy(key, field, incr);
    assertTrue(response1 == incr);

    long response2 = jedis.hincrBy(randString(), randString(), incr);
    assertTrue(response2 == incr);

    long response3 = jedis.hincrBy(key, field, incr);
    assertTrue(response3 + "=" + 2 * incr, response3 == 2 * incr);

    String field1 = randString();
    Exception ex = null;
    try {
      jedis.hincrBy(key, field1, Long.MAX_VALUE);
      jedis.hincrBy(key, field1, incr);
    } catch (Exception e) {
      ex = e;
    }

    assertNotNull(ex);
  }

  @Test
  public void testHIncrFloatBy() {
    String key = randString();
    String field = randString();

    DecimalFormat decimalFormat = new DecimalFormat("#.#####");
    double incr = rand.nextDouble();
    String incrAsString = decimalFormat.format(incr);
    incr = Double.valueOf(incrAsString);
    if (incr == 0) {
      incr = incr + 1;
    }

    Double response1 = jedis.hincrByFloat(key, field, incr);
    assertThat(response1).isEqualTo(incr, offset(.00001));

    assertThat(response1).isEqualTo(Double.valueOf(jedis.hget(key, field)), offset(.00001));

    double response2 = jedis.hincrByFloat(randString(), randString(), incr);

    assertThat(response2).isEqualTo(incr, offset(.00001));

    Double response3 = jedis.hincrByFloat(key, field, incr);
    assertThat(response3).isEqualTo(2 * incr, offset(.00001));

    assertThat(response3).isEqualTo(Double.valueOf(jedis.hget(key, field)), offset(.00001));

  }

  @Test
  public void testHExists() {
    String key = Double.valueOf(rand.nextDouble()).toString();
    String field = Double.valueOf(rand.nextInt(50)).toString() + ".field";
    String value = Double.valueOf(rand.nextInt(50)).toString() + ".value";

    assertFalse(jedis.hexists(key, field));

    jedis.hset(key, field, value);

    assertEquals(value, jedis.hget(key, field));

    Assert.assertTrue(jedis.hexists(key, field));

    key = "testObject:" + key;

    value = Double.valueOf(rand.nextInt(50)).toString() + ".value";
    jedis.hset(key, field, value);

    Assert.assertTrue(jedis.hexists(key, field));

    jedis.hdel(key, field);

    assertNull(jedis.hget(key, field));
    assertFalse(jedis.hexists(key, field));

  }

  @Test
  public void testHScan() {

    String key = Double.valueOf(rand.nextDouble()).toString();
    String field = Double.valueOf(rand.nextInt(50)).toString() + ".field";
    String value = Double.valueOf(rand.nextInt(50)).toString() + ".value";

    ScanResult<Entry<String, String>> results = null;

    try {
      results = jedis.hscan(key, "this cursor is non-numeric and so completely invalid");
      fail("Must throw exception for invalid cursor");
    } catch (Exception e) {
    }

    Map<String, String> hash = new HashMap<String, String>();
    hash.put(field, value);

    jedis.hmset(key, hash);

    results = jedis.hscan(key, "0");

    assertNotNull(results);
    assertNotNull(results.getResult());

    assertEquals(1, results.getResult().size());
    assertEquals(hash.entrySet().iterator().next(), results.getResult().iterator().next());

  }

  /**
   * Test for the HSetNX command
   */
  @Test
  public void testHSetNXExecutor() {
    String key = "HSetNX" + randString();
    String field = randString();
    String value = randString();

    // 1 if field is a new field in the hash and value was set.
    Long result = jedis.hsetnx(key, field, value);
    assertEquals(Long.valueOf(1), result);

    // test field value
    assertEquals(value, jedis.hget(key, field));

    result = jedis.hsetnx(key, field, "changedValue");
    assertEquals(Long.valueOf(0), result);

    assertEquals(value, jedis.hget(key, field));

    jedis.hdel(key, field);

    assertFalse(jedis.hexists(key, field));

  }

  /**
   * Test the HVALS command
   */
  @Test
  public void testHVals() throws Exception {
    String key = "HVals" + randString();
    String field1 = randString();
    String field2 = randString();
    String value = randString();

    List<String> list = jedis.hvals(key);
    assertTrue(list == null || list.isEmpty());

    Long result = jedis.hset(key, field1, value);
    assertEquals(Long.valueOf(1), result);

    result = jedis.hset(key, field2, value);
    assertEquals(Long.valueOf(1), result);
    list = jedis.hvals(key);

    assertNotNull(list);
    assertTrue(!list.isEmpty());
    assertEquals(2, list.size());

    list.contains(value);
  }

  /**
   * <pre>
   * Test HLEN
   *
   * Example
   *
   *
   * redis> HSET myhash field1 "Hello"
   * (integer) 1
   * redis> HSET myhash field2 "World"
   * (integer) 1
   * redis> HLEN myhash
   * (integer) 2
   * </pre>
   */
  @Test
  public void testHLen() {

    String key = "HLen" + randString();
    String field1 = randString();
    String field2 = randString();
    String value = randString();

    Long result = jedis.hlen(key); // check error handling when key does not exist

    result = jedis.hset(key, field1, value);
    assertEquals(Long.valueOf(1), result);

    result = jedis.hset(key, field2, value);
    assertEquals(Long.valueOf(1), result);

    result = jedis.hlen(key);
    assertEquals(Long.valueOf(2), result);

  }

  /**
   * <pre>
   * Test for HKeys
   *
   * redis> HSET myhash field1 "Hello"
   * (integer) 1
   * redis> HSET myhash field2 "World"
   * (integer) 1
   * redis> HKEYS myhash
   * 1) "field1"
   * 2) "field2"
   *
   * </pre>
   */
  @Test
  public void testHKeys() {
    String key = "HKeys" + randString();
    String field1 = randString();
    String field2 = randString();
    String field1Value = randString();
    String field2Value = randString();

    Set<String> set = jedis.hkeys(key);
    assertTrue(set == null || set.isEmpty());

    Long result = jedis.hset(key, field1, field1Value);
    assertEquals(Long.valueOf(1), result);

    result = jedis.hset(key, field2, field2Value);
    assertEquals(Long.valueOf(1), result);

    set = jedis.hkeys(key);
    assertNotNull(set);
    assertTrue(!set.isEmpty() && set.size() == 2);

    assertTrue(set.contains(field1));
    assertTrue(set.contains(field2));
  }

  /**
   * Test the Redis HGETALL command to return
   * <p>
   * Returns all fields and values of the hash stored at key.
   * <p>
   * Examples:
   * <p>
   * redis> HSET myhash field1 "Hello" (integer) 1 redis> HSET myhash field2 "World" (integer) 1
   * redis> HGETALL myhash 1) "field1" 2) "Hello" 3) "field2" 4) "World"
   */
  @Test
  public void testHGETALL() {

    String key = "HGETALL" + randString();

    Map<String, String> map = jedis.hgetAll(key);
    assertTrue(map == null || map.isEmpty());

    String field1 = randString();
    String field2 = randString();
    String field1Value = randString();
    String field2Value = randString();

    Long result = jedis.hset(key, field1, field1Value);
    assertEquals(Long.valueOf(1), result);

    result = jedis.hset(key, field2, field2Value);
    assertEquals(Long.valueOf(1), result);

    map = jedis.hgetAll(key);
    assertNotNull(map);

    assertTrue(!map.isEmpty() && map.size() == 2);

    assertTrue(map.keySet().contains(field1));
    assertTrue(map.keySet().contains(field2));

    assertTrue(map.values().contains(field1Value));
    assertTrue(map.values().contains(field2Value));
  }

  @Test
  public void testHsetHandlesMultipleFields() {
    String key = "HMSET" + randString();
    String field1 = "F1" + randString();
    String field2 = "F2" + randString();
    String field1Value = randString();
    String field2Value = randString();
    Long fieldsAdded;

    Map<String, String> hsetMap = new HashMap<>();
    hsetMap.put(field1, field1Value);
    hsetMap.put(field2, field2Value);

    fieldsAdded = jedis.hset(key, hsetMap);

    Map<String, String> result = jedis.hgetAll(key);

    assertThat(result).isEqualTo(hsetMap);
    assertThat(fieldsAdded).isEqualTo(2);

    fieldsAdded = jedis.hset(key, hsetMap);
    assertThat(fieldsAdded).isEqualTo(0);
  }

  @Test
  public void testConcurrentHMSet_differentKeyPerClient() throws InterruptedException {
    String key1 = "HMSET" + randString();
    String key2 = "HMSET" + randString();
    Map<String, String> record1 = new HashMap<String, String>();
    Map<String, String> record2 = new HashMap<String, String>();

    Runnable runnable1 = () -> doABunchOfHMSets(key1, record1, jedis);
    Runnable runnable2 = () -> doABunchOfHMSets(key2, record2, jedis2);
    Thread thread1 = new Thread(runnable1);
    Thread thread2 = new Thread(runnable2);
    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();

    Map<String, String> result = jedis.hgetAll(key1);
    assertEquals(record1.size(), result.size());
    assertThat(result.keySet().containsAll(record1.keySet())).isTrue();
    assertThat(result.values().containsAll(record1.values())).isTrue();

    Map<String, String> result2 = jedis.hgetAll(key2);
    assertEquals(record2.size(), result2.size());
    assertThat(result2.keySet().containsAll(record2.keySet())).isTrue();
    assertThat(result2.values().containsAll(record2.values())).isTrue();
  }

  @Test
  public void testConcurrentHMSet_sameKeyPerClient() throws InterruptedException {
    String key = "HMSET" + randString();
    Map<String, String> record1 = new HashMap<String, String>();
    Map<String, String> record2 = new HashMap<String, String>();

    Runnable runnable1 = () -> doABunchOfHMSets(key, record1, jedis);
    Runnable runnable2 = () -> doABunchOfHMSets(key, record2, jedis2);
    Thread thread1 = new Thread(runnable1);
    Thread thread2 = new Thread(runnable2);
    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();

    Map<String, String> result = jedis.hgetAll(key);
    assertEquals(record1.size() + record2.size(), result.size());
    assertThat(result.keySet().containsAll(record1.keySet())).isTrue();
    assertThat(result.keySet().containsAll(record2.keySet())).isTrue();
    assertThat(result.values().containsAll(record1.values())).isTrue();
    assertThat(result.values().containsAll(record2.values())).isTrue();
  }

  @Test
  public void testConcurrentHSetNX() throws InterruptedException, ExecutionException {
    String key = "HSETNX" + randString();

    ArrayList<String> fields = new ArrayList<>(ITERATION_COUNT);

    for (int i = 0; i < ITERATION_COUNT; i++) {
      fields.add(randString());
    }


    CountDownLatch latch = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    Callable<Integer> callable1 = () -> doABunchOfHSetNXs(key, fields, "Thread1", jedis, latch);
    Callable<Integer> callable2 = () -> doABunchOfHSetNXs(key, fields, "Thread2", jedis2, latch);
    Future<Integer> future1 = pool.submit(callable1);
    Future<Integer> future2 = pool.submit(callable2);

    latch.countDown();
    assertThat(future1.get() + future2.get()).isEqualTo(ITERATION_COUNT);

    pool.shutdown();
  }

  private int doABunchOfHSetNXs(String key, ArrayList<String> fields, String fieldValue,
      Jedis jedis, CountDownLatch latch) throws InterruptedException {
    int successes = 0;

    latch.await();
    for (int i = 0; i < ITERATION_COUNT; i++) {
      if (jedis.hsetnx(key, fields.get(i), fieldValue) == 1) {
        successes++;
        Thread.yield();
      }
    }
    return successes;
  }

  @Test
  public void testConcurrentHSet_differentKeyPerClient() throws InterruptedException {
    String key1 = "HSET" + randString();
    String key2 = "HSET" + randString();
    Map<String, String> record1 = new HashMap<String, String>();
    Map<String, String> record2 = new HashMap<String, String>();

    Runnable runnable1 = () -> doABunchOfHSets(key1, record1, jedis);
    Runnable runnable2 = () -> doABunchOfHSets(key2, record2, jedis2);
    Thread thread1 = new Thread(runnable1);
    Thread thread2 = new Thread(runnable2);
    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();

    Map<String, String> result = jedis.hgetAll(key1);
    assertEquals(record1.size(), result.size());
    assertThat(result.keySet().containsAll(record1.keySet())).isTrue();
    assertThat(result.values().containsAll(record1.values())).isTrue();

    Map<String, String> result2 = jedis.hgetAll(key2);
    assertEquals(record2.size(), result2.size());
    assertThat(result2.keySet().containsAll(record2.keySet())).isTrue();
    assertThat(result2.values().containsAll(record2.values())).isTrue();
  }

  @Test
  public void testConcurrentHSet_sameKeyPerClient() throws InterruptedException {
    String key1 = "HSET" + randString();
    Map<String, String> record1 = new HashMap<String, String>();
    Map<String, String> record2 = new HashMap<String, String>();

    Runnable runnable1 = () -> doABunchOfHSets(key1, record1, jedis);
    Runnable runnable2 = () -> doABunchOfHSets(key1, record2, jedis2);
    Thread thread1 = new Thread(runnable1);
    Thread thread2 = new Thread(runnable2);
    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();

    Map<String, String> result = jedis.hgetAll(key1);
    assertEquals(record1.size() + record2.size(), result.size());
    assertThat(result.keySet().containsAll(record1.keySet())).isTrue();
    assertThat(result.keySet().containsAll(record2.keySet())).isTrue();
    assertThat(result.values().containsAll(record1.values())).isTrue();
    assertThat(result.values().containsAll(record2.values())).isTrue();
  }

  @Test
  public void testConcurrentHIncr_sameKeyPerClient() throws InterruptedException {
    String key1 = "HSET" + randString();
    String field = "FIELD" + randString();


    jedis.hset(key1, field, "0");

    Runnable runnable1 = () -> doABunchOfHIncrs(key1, field, ITERATION_COUNT / 2, jedis);
    Runnable runnable2 = () -> doABunchOfHIncrs(key1, field, ITERATION_COUNT / 2, jedis2);
    Thread thread1 = new Thread(runnable1);
    Thread thread2 = new Thread(runnable2);
    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();

    String value = jedis.hget(key1, field);
    assertThat(value).isEqualTo(Integer.toString(ITERATION_COUNT));
  }

  private void doABunchOfHIncrs(String key, String field, int incrCount, Jedis jedis) {
    for (int i = 0; i < incrCount; i++) {
      jedis.hincrBy(key, field, 1);
    }
  }

  @Test
  public void testConcurrentHIncrByFloat_sameKeyPerClient() throws InterruptedException {
    String key = "HSET" + randString();
    String field = "FIELD" + randString();


    jedis.hset(key, field, "0");

    Runnable runnable1 = () -> doABunchOfHIncrByFloats(key, field, ITERATION_COUNT / 2, 1.0, jedis);
    Runnable runnable2 =
        () -> doABunchOfHIncrByFloats(key, field, ITERATION_COUNT / 2, 0.5, jedis2);
    Thread thread1 = new Thread(runnable1);
    Thread thread2 = new Thread(runnable2);
    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();

    String value = jedis.hget(key, field);
    assertThat(value).isEqualTo(String.format("%.0f", ITERATION_COUNT * 0.75));
  }

  private void doABunchOfHIncrByFloats(String key, String field, int incrCount, double incrValue,
      Jedis jedis) {
    for (int i = 0; i < incrCount; i++) {
      jedis.hincrByFloat(key, field, incrValue);
    }
  }


  @Test
  public void testConcurrentHSetHDel_sameKeyPerClient() throws InterruptedException {
    String key1 = "HSET" + randString();

    ArrayBlockingQueue<String> blockingQueue = new ArrayBlockingQueue<>(ITERATION_COUNT);

    Runnable runnable1 = () -> doABunchOfHSetsWithBlockingQueue(key1, blockingQueue, jedis);
    Runnable runnable2 = () -> doABunchOfHDelsWithBlockingQueue(key1, blockingQueue, jedis2);
    Thread thread1 = new Thread(runnable1);
    Thread thread2 = new Thread(runnable2);
    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();

    Map<String, String> result = jedis.hgetAll(key1);
    assertThat(result).isEmpty();
  }

  @Test
  public void testConcurrentHGetAll() throws InterruptedException, ExecutionException {
    String key1 = "HSET" + randString();
    HashMap<String, String> record = new HashMap<>();

    doABunchOfHSets(key1, record, jedis);

    ExecutorService pool = Executors.newFixedThreadPool(2);
    Callable<Integer> callable1 = () -> doABunchOfHGetAlls(key1, jedis);
    Callable<Integer> callable2 = () -> doABunchOfHGetAlls(key1, jedis2);
    Future<Integer> future1 = pool.submit(callable1);
    Future<Integer> future2 = pool.submit(callable2);

    assertThat(future1.get()).isEqualTo(ITERATION_COUNT);
    assertThat(future2.get()).isEqualTo(ITERATION_COUNT);

    pool.shutdown();
  }

  private int doABunchOfHGetAlls(String key, Jedis jedis) {
    int returnedCount = 0;
    for (int i = 0; i < ITERATION_COUNT; i++) {
      if (jedis.hgetAll(key).size() == ITERATION_COUNT) {
        returnedCount++;
      }
    }
    return returnedCount;
  }

  private void doABunchOfHDelsWithBlockingQueue(String key,
      ArrayBlockingQueue<String> blockingQueue,
      Jedis jedis) {
    String field;
    for (int i = 0; i < ITERATION_COUNT; i++) {
      try {
        field = blockingQueue.take();
      } catch (InterruptedException e) {
        throw new RuntimeException("HDel thread was interrupted unexpectedly", e);
      }
      jedis.hdel(key, field);
    }
  }

  private void doABunchOfHSetsWithBlockingQueue(String key,
      ArrayBlockingQueue<String> blockingQueue,
      Jedis jedis) {
    String field;
    String fieldValue;
    for (int i = 0; i < ITERATION_COUNT; i++) {
      field = randString();
      fieldValue = randString();

      jedis.hset(key, field, fieldValue);

      blockingQueue.add(field);
    }
  }

  private void doABunchOfHSets(String key, Map<String, String> record, Jedis jedis) {
    String field;
    String fieldValue;
    for (int i = 0; i < ITERATION_COUNT; i++) {
      field = randString();
      fieldValue = randString();

      record.put(field, fieldValue);

      jedis.hset(key, field, fieldValue);
    }
  }

  private void doABunchOfHMSets(String key, Map<String, String> record, Jedis jedis) {
    String field;
    String fieldValue;
    for (int i = 0; i < ITERATION_COUNT; i++) {
      field = randString();
      fieldValue = randString();
      Map<String, String> hmsetMap = new HashMap<>();
      hmsetMap.put(field, fieldValue);

      record.put(field, fieldValue);

      jedis.hmset(key, hmsetMap);
    }
  }

  private String randString() {
    int length = rand.nextInt(8) + 5;
    return RandomStringUtils.randomAlphanumeric(length);
  }

  private int randSleepMillis() {
    return rand.nextInt(10) + 5;
  }

  @After
  public void flushAll() {
    jedis.flushAll();
    jedis2.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();
    jedis2.close();
    cache.close();
    server.shutdown();
  }
}
