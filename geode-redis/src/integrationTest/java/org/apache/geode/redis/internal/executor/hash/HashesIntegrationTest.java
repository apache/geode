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
package org.apache.geode.redis.internal.executor.hash;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.offset;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.RandomStringUtils;
import org.assertj.core.util.Maps;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.exceptions.JedisDataException;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.GeodeRedisServerRule;
import org.apache.geode.test.junit.categories.RedisTest;

@Category({RedisTest.class})
public class HashesIntegrationTest {
  static Random rand;
  static Jedis jedis;
  static Jedis jedis2;
  private static int ITERATION_COUNT = 4000;

  @ClassRule
  public static GeodeRedisServerRule server = new GeodeRedisServerRule();

  @BeforeClass
  public static void setUp() throws IOException {
    rand = new Random();
    jedis = new Jedis("localhost", server.getPort(), 10000000);
    jedis2 = new Jedis("localhost", server.getPort(), 10000000);
  }

  @After
  public void flushAll() {
    jedis.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();
    jedis2.close();
  }

  @Test
  public void testHMSet_givenWrongNumberOfArguments() {
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HMSET))
        .hasMessageContaining("wrong number of arguments");
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HMSET, "1"))
        .hasMessageContaining("wrong number of arguments");
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HMSET, "1", "2"))
        .hasMessageContaining("wrong number of arguments");
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HMSET, "1", "2", "3", "4"))
        .hasMessageContaining("wrong number of arguments");
  }

  @Test
  public void testHSet_givenWrongNumberOfArguments() {
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HSET))
        .hasMessageContaining("wrong number of arguments");
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HSET, "1"))
        .hasMessageContaining("wrong number of arguments");
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HSET, "1", "2"))
        .hasMessageContaining("wrong number of arguments");
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HSET, "1", "2", "3", "4"))
        .hasMessageContaining("wrong number of arguments");
  }

  @Test
  public void testHGetall_givenWrongNumberOfArguments() {
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HGETALL))
        .hasMessageContaining("wrong number of arguments");
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HMSET, "1", "2"))
        .hasMessageContaining("wrong number of arguments");
  }

  @Test
  public void testHMSet() {
    int num = 10;
    String key = randString();
    Map<String, String> hash = new HashMap<String, String>();
    for (int i = 0; i < num; i++) {
      hash.put(randString(), randString());
    }
    String response = jedis.hmset(key, hash);
    assertThat(response).isEqualTo("OK");
    assertThat(jedis.hlen(key)).isEqualTo(hash.size());
  }

  @Test
  public void testHSet() {
    String key = randString();
    Map<String, String> hash = new HashMap<String, String>();

    for (int i = 0; i < 10; i++) {
      hash.put(randString(), randString());
    }
    Set<String> keys = hash.keySet();
    Long count = 1L;
    for (String field : keys) {
      Long res = jedis.hset(key, field, hash.get(field));
      assertThat(res).isEqualTo(1);
      assertThat(jedis.hlen(key)).isEqualTo(count);

      count += 1;
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
      assertThat(hash.get(keyArray[i])).isEqualTo(retList.get(i));
    }

    Map<String, String> retMap = jedis.hgetAll(key);

    assertThat(retMap).containsExactlyInAnyOrderEntriesOf(hash);

    List<String> retVals = jedis.hvals(key);
    Set<String> retSet = new HashSet<String>(retVals);

    assertThat(retSet.containsAll(hash.values())).isTrue();

    jedis.hdel(key, keyArray);
    assertThat(jedis.hlen(key)).isEqualTo(0);
  }

  @Test
  public void testHDelErrorMessage_givenIncorrectDataType() {
    jedis.set("farm", "chicken");
    assertThatThrownBy(() -> {
      jedis.hdel("farm", "chicken");
    }).isInstanceOf(JedisDataException.class)
        .hasMessageContaining("WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  @Test
  public void testHDelDeletesKeyWhenHashIsEmpty() {
    jedis.hset("farm", "chicken", "little");

    jedis.hdel("farm", "chicken");

    assertThat(jedis.exists("farm")).isFalse();
  }

  @Test
  public void testHStrLen() {
    jedis.hset("farm", "chicken", "little");

    assertThat(jedis.hstrlen("farm", "chicken")).isEqualTo("little".length());
    assertThat(jedis.hstrlen("farm", "unknown-field")).isEqualTo(0);
    assertThat(jedis.hstrlen("unknown-key", "unknown-field")).isEqualTo(0);
  }

  @Test
  public void testHStrLen_failsForNonHashes() {
    jedis.sadd("farm", "chicken");
    assertThatThrownBy(() -> jedis.hstrlen("farm", "chicken"))
        .hasMessageContaining("WRONGTYPE");

    jedis.set("tractor", "John Deere");
    assertThatThrownBy(() -> jedis.hstrlen("tractor", "chicken"))
        .hasMessageContaining("WRONGTYPE");
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

    assertThat(retSet.containsAll(keys)).isTrue();
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
    assertThat(response1).isEqualTo(incr);

    long response2 = jedis.hincrBy(randString(), randString(), incr);
    assertThat(response2).isEqualTo(incr);

    long response3 = jedis.hincrBy(key, field, incr);
    assertThat(response3).as(response3 + "=" + 2 * incr)
        .isEqualTo(2 * incr);

    String field1 = randString();
    long myincr = incr;
    assertThatThrownBy(() -> {
      jedis.hincrBy(key, field1, Long.MAX_VALUE);
      jedis.hincrBy(key, field1, myincr);
    }).isInstanceOf(JedisDataException.class)
        .hasMessageContaining("ERR increment or decrement would overflow");
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
  public void incrByFloatFailsWithNonFloatFieldValue() {
    String key = randString();
    String field = randString();
    jedis.hset(key, field, "foobar");
    assertThatThrownBy(() -> {
      jedis.hincrByFloat(key, field, 1.5);
    }).isInstanceOf(JedisDataException.class)
        .hasMessageContaining("ERR hash value is not a float");
  }

  @Test
  public void testHExists() {
    String key = Double.valueOf(rand.nextDouble()).toString();
    String field = Double.valueOf(rand.nextInt(50)).toString() + ".field";
    String value = Double.valueOf(rand.nextInt(50)).toString() + ".value";

    assertThat(jedis.hexists(key, field)).isFalse();

    jedis.hset(key, field, value);

    assertThat(jedis.hget(key, field)).isEqualTo(value);

    assertThat(jedis.hexists(key, field)).isTrue();

    key = "testObject:" + key;

    value = Double.valueOf(rand.nextInt(50)).toString() + ".value";
    jedis.hset(key, field, value);

    assertThat(jedis.hexists(key, field)).isTrue();

    jedis.hdel(key, field);

    assertThat(jedis.hget(key, field)).isNull();
    assertThat(jedis.hexists(key, field)).isFalse();

  }

  @Test
  public void testHScan() {

    String key = Double.valueOf(rand.nextDouble()).toString();
    String field = Double.valueOf(rand.nextInt(50)).toString() + ".field";
    String value = Double.valueOf(rand.nextInt(50)).toString() + ".value";

    ScanResult<Entry<String, String>> results = null;

    assertThatThrownBy(
        () -> jedis.hscan(key, "this cursor is non-numeric and so completely invalid"))
            .hasMessageContaining("invalid cursor");

    Map<String, String> hash = new HashMap<>();
    hash.put(field, value);

    jedis.hmset(key, hash);

    results = jedis.hscan(key, "0");

    assertThat(results).isNotNull();
    assertThat(results.getResult()).isNotNull();

    assertThat(results.getResult().size()).isEqualTo(1);
    assertThat(results.getResult()).containsExactlyInAnyOrderElementsOf(hash.entrySet());
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
    assertThat(result).isEqualTo(1);

    // test field value
    assertThat(jedis.hget(key, field)).isEqualTo(value);

    result = jedis.hsetnx(key, field, "changedValue");
    assertThat(result).isEqualTo(0);

    assertThat(jedis.hget(key, field)).isEqualTo(value);

    jedis.hdel(key, field);

    assertThat(jedis.hexists(key, field)).isFalse();

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
    assertThat(list == null || list.isEmpty()).isTrue();

    Long result = jedis.hset(key, field1, value);
    assertThat(result).isEqualTo(1);

    result = jedis.hset(key, field2, value);
    assertThat(result).isEqualTo(1);
    list = jedis.hvals(key);

    assertThat(list).isNotNull();
    assertThat(list).isNotEmpty();
    assertThat(list).hasSize(2);

    assertThat(list).contains(value);
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
    assertThat(result).isEqualTo(1);

    result = jedis.hset(key, field2, value);
    assertThat(result).isEqualTo(1);

    result = jedis.hlen(key);
    assertThat(result).isEqualTo(2);

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
    assertThat(set == null || set.isEmpty()).isTrue();

    Long result = jedis.hset(key, field1, field1Value);
    assertThat(result).isEqualTo(1);

    result = jedis.hset(key, field2, field2Value);
    assertThat(result).isEqualTo(1);

    set = jedis.hkeys(key);
    assertThat(set).isNotNull();
    assertThat(set).hasSize(2);

    assertThat(set).contains(field1);
    assertThat(set).contains(field2);
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
    assertThat(map == null || map.isEmpty()).isTrue();

    String field1 = randString();
    String field2 = randString();
    String field1Value = randString();
    String field2Value = randString();

    Long result = jedis.hset(key, field1, field1Value);
    assertThat(result).isEqualTo(1);

    result = jedis.hset(key, field2, field2Value);
    assertThat(result).isEqualTo(1);

    map = jedis.hgetAll(key);
    assertThat(map).isNotNull();

    assertThat(map).hasSize(2);
    assertThat(map.keySet()).containsExactlyInAnyOrder(field1, field2);
    assertThat(map.values()).containsExactlyInAnyOrder(field1Value, field2Value);
  }

  @Test
  public void testHsetHandlesMultipleFields() {
    String key = "key" + randString();

    Long fieldsAdded;

    Map<String, String> hsetMap = new HashMap<>();
    hsetMap.put(randString(), randString());
    hsetMap.put(randString(), randString());

    fieldsAdded = jedis.hset(key, hsetMap);

    Map<String, String> result = jedis.hgetAll(key);

    assertThat(result).isEqualTo(hsetMap);
    assertThat(fieldsAdded).isEqualTo(2);

    fieldsAdded = jedis.hset(key, hsetMap);
    assertThat(fieldsAdded).isEqualTo(0);
  }

  @Test
  public void testConcurrentHMSet_differentKeyPerClient() {
    String key1 = "HMSET1";
    String key2 = "HMSET2";
    Map<String, String> expectedMap = new HashMap<>();
    for (int i = 0; i < ITERATION_COUNT; i++) {
      expectedMap.put("field" + i, "value" + i);
    }

    new ConcurrentLoopingThreads(ITERATION_COUNT,
        (i) -> jedis.hmset(key1, Maps.newHashMap("field" + i, "value" + i)),
        (i) -> jedis2.hmset(key2, Maps.newHashMap("field" + i, "value" + i)))
            .run();

    assertThat(jedis.hgetAll(key1)).isEqualTo(expectedMap);
    assertThat(jedis2.hgetAll(key2)).isEqualTo(expectedMap);
  }

  @Test
  public void testConcurrentHMSet_sameKeyPerClient() {
    String key = "HMSET1";

    new ConcurrentLoopingThreads(ITERATION_COUNT,
        (i) -> jedis.hmset(key, Maps.newHashMap("fieldA" + i, "valueA" + i)),
        (i) -> jedis2.hmset(key, Maps.newHashMap("fieldB" + i, "valueB" + i)))
            .run();

    Map<String, String> result = jedis.hgetAll(key);
    assertThat(result).hasSize(ITERATION_COUNT * 2);
  }

  @Test
  public void testConcurrentHSetNX() {
    String key = "HSETNX" + randString();

    AtomicLong successCount = new AtomicLong();
    new ConcurrentLoopingThreads(ITERATION_COUNT,
        (i) -> successCount.addAndGet(jedis.hsetnx(key, "field" + i, "A")),
        (i) -> successCount.addAndGet(jedis2.hsetnx(key, "field" + i, "B")))
            .run();

    assertThat(successCount.get()).isEqualTo(ITERATION_COUNT);
  }

  @Test
  public void testConcurrentHSet_differentKeyPerClient() {
    String key1 = "HSET1";
    String key2 = "HSET2";
    Map<String, String> expectedMap = new HashMap<String, String>();
    for (int i = 0; i < ITERATION_COUNT; i++) {
      expectedMap.put("field" + i, "value" + i);
    }

    new ConcurrentLoopingThreads(ITERATION_COUNT,
        (i) -> jedis.hset(key1, "field" + i, "value" + i),
        (i) -> jedis2.hset(key2, "field" + i, "value" + i))
            .run();

    assertThat(jedis.hgetAll(key1)).isEqualTo(expectedMap);
    assertThat(jedis.hgetAll(key2)).isEqualTo(expectedMap);
  }

  @Test
  public void testConcurrentHSet_sameKeyPerClient() throws InterruptedException {
    String key1 = "HSET1";

    new ConcurrentLoopingThreads(ITERATION_COUNT,
        (i) -> jedis.hset(key1, "fieldA" + i, "value" + i),
        (i) -> jedis2.hset(key1, "fieldB" + i, "value" + i))
            .run();
    Map<String, String> result = jedis.hgetAll(key1);

    assertThat(result).hasSize(ITERATION_COUNT * 2);
  }

  @Test
  public void testConcurrentHIncr_sameKeyPerClient() throws InterruptedException {
    String key = "KEY";
    String field = "FIELD";

    jedis.hset(key, field, "0");

    new ConcurrentLoopingThreads(ITERATION_COUNT,
        (i) -> jedis.hincrBy(key, field, 1),
        (i) -> jedis2.hincrBy(key, field, 1))
            .run();

    String value = jedis.hget(key, field);
    assertThat(value).isEqualTo(Integer.toString(ITERATION_COUNT * 2));
  }

  @Test
  public void testConcurrentHIncrByFloat_sameKeyPerClient() throws InterruptedException {
    String key = "HSET" + randString();
    String field = "FIELD" + randString();

    jedis.hset(key, field, "0");

    new ConcurrentLoopingThreads(ITERATION_COUNT,
        (i) -> jedis.hincrByFloat(key, field, 0.5),
        (i) -> jedis2.hincrByFloat(key, field, 1.0)).run();

    String value = jedis.hget(key, field);
    assertThat(value).isEqualTo(String.format("%.0f", ITERATION_COUNT * 1.5));
  }

  @Test
  public void testHSet_keyExistsWithDifferentDataType() {
    jedis.set("key", "value");

    assertThatThrownBy(
        () -> jedis.hset("key", "field", "something else")).isInstanceOf(JedisDataException.class)
            .hasMessageContaining("WRONGTYPE");
  }

  @Test
  public void testConcurrentHSetHDel_sameKeyPerClient() {
    String key = "HSET1";

    ArrayBlockingQueue<String> blockingQueue = new ArrayBlockingQueue<>(ITERATION_COUNT);

    new ConcurrentLoopingThreads(ITERATION_COUNT,
        (i) -> {
          jedis.hset(key, "field" + i, "value" + i);
          blockingQueue.add("field" + i);
        },
        (i) -> {
          try {
            String fieldToDelete = blockingQueue.take();
            jedis2.hdel(key, fieldToDelete);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        })
            .run();

    Map<String, String> result = jedis.hgetAll(key);

    assertThat(result).isEmpty();
  }

  @Test
  public void testConcurrentHGetAll() {
    String key = "HSET1";
    HashMap<String, String> record = new HashMap<>();

    doABunchOfHSets(key, record, jedis);

    AtomicLong successCount = new AtomicLong();
    new ConcurrentLoopingThreads(ITERATION_COUNT,
        (i) -> {
          if (jedis.hgetAll(key).size() == ITERATION_COUNT) {
            successCount.incrementAndGet();
          }
        },
        (i) -> {
          if (jedis2.hgetAll(key).size() == ITERATION_COUNT) {
            successCount.incrementAndGet();
          }
        })
            .run();

    assertThat(successCount.get()).isEqualTo(ITERATION_COUNT * 2);
  }

  @Test
  public void testHset_canStoreBinaryData() {
    byte[] blob = new byte[256];
    for (int i = 0; i < 256; i++) {
      blob[i] = (byte) i;
    }

    jedis.hset("key".getBytes(), blob, blob);
    Map<byte[], byte[]> result = jedis.hgetAll("key".getBytes());

    assertThat(result.keySet()).containsExactly(blob);
    assertThat(result.values()).containsExactly(blob);
  }

  @Test
  public void testHstrlen_withBinaryData() {
    byte[] zero = new byte[] {0};
    jedis.hset(zero, zero, zero);

    assertThat(jedis.hstrlen(zero, zero)).isEqualTo(1);
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

  private String randString() {
    int length = rand.nextInt(8) + 5;
    return RandomStringUtils.randomAlphanumeric(length);
  }

}
