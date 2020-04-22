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

package org.apache.geode.redis.general;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.redis.GeodeRedisServer;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public class ExistsIntegrationTest {

  public static Jedis jedis;
  public static Jedis jedis2;
  public static Jedis jedis3;
  public static int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());;
  private static GeodeRedisServer server;
  private static GemFireCache cache;

  @BeforeClass
  public static void setUp() {
    CacheFactory cf = new CacheFactory();
    cf.set(LOG_LEVEL, "error");
    cf.set(MCAST_PORT, "0");
    cf.set(LOCATORS, "");
    cache = cf.create();
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    server = new GeodeRedisServer("localhost", port);

    server.start();
    jedis = new Jedis("localhost", port, REDIS_CLIENT_TIMEOUT);
    jedis2 = new Jedis("localhost", port, REDIS_CLIENT_TIMEOUT);
    jedis3 = new Jedis("localhost", port, REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void flushAll() {
    jedis.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();
    jedis2.close();
    jedis3.close();
    cache.close();
    server.shutdown();
  }

  @Test
  public void shouldReturnZero_givenKeyDoesNotExist() {
    assertThat(jedis.exists(toArray("doesNotExist"))).isEqualTo(0L);
  }

  @Test
  public void shouldReturn1_givenStringExists() {
    String stringKey = "stringKey";
    String stringValue = "stringValue";
    jedis.set(stringKey, stringValue);

    assertThat(jedis.exists(toArray(stringKey))).isEqualTo(1L);
  }

  @Test
  public void shouldReturn0_givenStringDoesNotExist() {
    String stringKey = "stringKey";
    String stringValue = "stringValue";
    jedis.set(stringKey, stringValue);
    jedis.del(stringKey);

    assertThat(jedis.exists(toArray(stringKey))).isEqualTo(0L);
  }

  @Test
  public void shouldReturn1_givenSetExists() {
    String setKey = "setKey";
    String setMember = "setValue";

    jedis.sadd(setKey, setMember);

    assertThat(jedis.exists(toArray(setKey))).isEqualTo(1L);
  }

  @Test
  public void shouldReturn0_givenSetDoesNotExist() {
    String setKey = "setKey";
    String setMember = "setValue";

    jedis.sadd(setKey, setMember);
    jedis.del(setKey);

    assertThat(jedis.exists(toArray(setKey))).isEqualTo(0L);
  }

  @Test
  public void shouldReturn1_givenSortedSetExists() {
    String sortedSetKey = "sortedSetKey";
    double score = 2.0;
    String sortedSetMember = "sortedSetMember";

    jedis.zadd(sortedSetKey, score, sortedSetMember);

    assertThat(jedis.exists(toArray(sortedSetKey))).isEqualTo(1L);
  }

  @Test
  public void shouldReturn0_givenSortedSetDoesNotExist() {
    String sortedSetKey = "sortedSetKey";
    double score = 2.0;
    String sortedSetMember = "sortedSetMember";

    jedis.zadd(sortedSetKey, score, sortedSetMember);
    jedis.del(sortedSetKey);

    assertThat(jedis.exists(toArray(sortedSetKey))).isEqualTo(0L);
  }

  @Test
  public void shouldReturn1_givenHashExists() {
    String hashKey = "hashKey";
    String hashField = "hashField";
    String hashValue = "hashValue";

    jedis.hset(hashKey, hashField, hashValue);

    assertThat(jedis.exists(toArray(hashKey))).isEqualTo(1L);
  }

  @Test
  public void shouldReturn0_givenHashDoesNotExist() {
    String hashKey = "hashKey";
    String hashField = "hashField";
    String hashValue = "hashValue";

    jedis.hset(hashKey, hashField, hashValue);
    jedis.del(hashKey);

    assertThat(jedis.exists(toArray(hashKey))).isEqualTo(0L);
  }

  @Test
  public void shouldReturn1_givenGeoExists() {
    String geoKey = "sicily";
    double latitude = 13.361389;
    double longitude = 38.115556;
    String geoMember = "Palermo Catania";

    jedis.geoadd(geoKey, latitude, longitude, geoMember);

    assertThat(jedis.exists(toArray(geoKey))).isEqualTo(1L);
  }

  @Test
  public void shouldReturn0_givenGeoDoesNotExist() {
    String geoKey = "sicily";
    double latitude = 13.361389;
    double longitude = 38.115556;
    String geoMember = "Palermo Catania";

    jedis.geoadd(geoKey, latitude, longitude, geoMember);
    jedis.del(geoKey);

    assertThat(jedis.exists(toArray(geoKey))).isEqualTo(0L);
  }

  @Test
  public void shouldReturn1_givenHyperLogLogExists() {
    String hyperLogLogKey = "crawled:127.0.0.2";
    String hyperLogLogValue = "www.insideTheHouse.com";

    jedis.pfadd(hyperLogLogKey, hyperLogLogValue);

    assertThat(jedis.exists(toArray(hyperLogLogKey))).isEqualTo(1L);
  }

  @Test
  public void shouldReturn0_givenHyperLogLogDoesNotExist() {
    String hyperLogLogKey = "crawled:127.0.0.2";
    String hyperLogLogValue = "www.insideTheHouse.com";

    jedis.pfadd(hyperLogLogKey, hyperLogLogValue);
    jedis.del(hyperLogLogKey);

    assertThat(jedis.exists(toArray(hyperLogLogKey))).isEqualTo(0L);
  }

  @Test
  public void shouldReturn1_givenListExists() {
    String listKey = "listKey";
    String listValue = "listValue";

    jedis.lpush(listKey, listValue);

    assertThat(jedis.exists(toArray(listKey))).isEqualTo(1L);
  }

  @Test
  public void shouldReturn0_givenListDoesNotExist() {
    String listKey = "listKey";
    String listValue = "listValue";

    jedis.lpush(listKey, listValue);
    jedis.del(listKey);

    assertThat(jedis.exists(toArray(listKey))).isEqualTo(0L);
  }

  @Test
  public void shouldReturn1_givenBitMapExists() {
    String bitMapKey = "bitMapKey";
    long offset = 1L;
    String bitMapValue = "0";

    jedis.setbit(bitMapKey, offset, bitMapValue);

    assertThat(jedis.exists(toArray(bitMapKey))).isEqualTo(1L);
  }

  @Test
  public void shouldReturn0_givenBitMapDoesNotExist() {
    String bitMapKey = "bitMapKey";
    long offset = 1L;
    String bitMapValue = "0";

    jedis.setbit(bitMapKey, offset, bitMapValue);
    jedis.del(bitMapKey);

    assertThat(jedis.exists(toArray(bitMapKey))).isEqualTo(0L);
  }

  @Test
  public void shouldReturnTotalNumber_givenMultipleKeys() {
    String key1 = "key1";
    String key2 = "key2";

    jedis.set(key1, "value1");
    jedis.set(key2, "value2");

    assertThat(jedis.exists(toArray(key1, "doesNotExist1", key2, "doesNotExist2"))).isEqualTo(2L);
  }

  @Test
  public void shouldCorrectlyVerifyKeysExistConcurrently() {
    int iterationCount = 5000;

    new ConcurrentLoopingThreads(iterationCount, (i) -> jedis.set("key" + i, "value" + i)).run();

    AtomicLong existsCount = new AtomicLong(0);
    new ConcurrentLoopingThreads(
        iterationCount,
        (i) -> existsCount.addAndGet(jedis.exists(toArray("key" + i))),
        (i) -> existsCount.addAndGet(jedis2.exists(toArray("key" + i))))
            .run();

    assertThat(existsCount.get()).isEqualTo(2 * iterationCount);
  }

  @Test
  public void shouldNotThrowExceptionsWhenConcurrentlyCreatingCheckingAndDeletingKeys() {

    int iterationCount = 5000;
    new ConcurrentLoopingThreads(
        iterationCount,
        (i) -> jedis.set("key", "value"),
        (i) -> jedis2.exists(toArray("key")),
        (i) -> jedis3.del("key"))
            .run();
  }

  public String[] toArray(String... strings) {
    return strings;
  }


}
