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
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanResult;

@Category(IntegrationTest.class)
public class HashesJUnitTest {
  private static Jedis jedis;
  private static GeodeRedisServer server;
  private static GemFireCache cache;
  private static Random rand;
  private static int port = 6379;

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
    server = new GeodeRedisServer("localhost", port);

    server.start();
    jedis = new Jedis("localhost", port, 10000000);
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
    if (incr == 0)
      incr++;

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

    double incr = rand.nextDouble();
    if (incr == 0)
      incr = incr + 1;

    Double response1 = jedis.hincrByFloat(key, field, incr);
    assertTrue(response1 == incr);

    assertEquals(response1, Double.valueOf(jedis.hget(key, field)));

    double response2 = jedis.hincrByFloat(randString(), randString(), incr);

    assertTrue(response2 == incr);

    Double response3 = jedis.hincrByFloat(key, field, incr);
    assertTrue(response3 + "=" + 2 * incr, response3 == 2 * incr);

    assertEquals(response3, Double.valueOf(jedis.hget(key, field)));

  }

  @Test
  public void testHExists() {
    double incr = rand.nextDouble();
    if (incr == 0)
      incr = incr + 1;

    String key = Double.valueOf(rand.nextDouble()).toString();
    String field = Double.valueOf(rand.nextInt(50)).toString() + ".field";
    String value = Double.valueOf(rand.nextInt(50)).toString() + ".value";

    Assert.assertFalse(jedis.hexists(key, field));

    jedis.hset(key, field, value);

    assertEquals(value, jedis.hget(key, field));

    Assert.assertTrue(jedis.hexists(key, field));

    key = "testObject:" + key;

    value = Double.valueOf(rand.nextInt(50)).toString() + ".value";
    jedis.hset(key, field, value);

    Assert.assertTrue(jedis.hexists(key, field));

    jedis.hdel(key, field);

    assertNull(jedis.hget(key, field));
    Assert.assertFalse(jedis.hexists(key, field));

  }

  @Test
  public void testHScan() {

    String key = Double.valueOf(rand.nextDouble()).toString();
    String field = Double.valueOf(rand.nextInt(50)).toString() + ".field";
    String value = Double.valueOf(rand.nextInt(50)).toString() + ".value";

    ScanResult<Entry<String, String>> results = null;


    try {
      results = jedis.hscan(key, "0");
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
   * 
   * @throws Exception
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
   * 
   * Returns all fields and values of the hash stored at key.
   * 
   * Examples:
   * 
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

  private String randString() {
    int length = rand.nextInt(8) + 5;
    StringBuilder rString = new StringBuilder();
    for (int i = 0; i < length; i++)
      rString.append((char) (rand.nextInt(57) + 65));
    return rString.toString();
  }

  @After
  public void flushAll() {
    jedis.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();
    cache.close();
    server.shutdown();
  }
}
