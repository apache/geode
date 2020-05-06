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
import redis.clients.jedis.params.SetParams;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.redis.GeodeRedisServer;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public class PersistIntegrationTest {

  public static Jedis jedis;
  public static Jedis jedis2;
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
  }

  @After
  public void flushAll() {
    jedis.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();
    jedis2.close();
    cache.close();
    server.shutdown();
  }

  @Test
  public void shouldPersistKey_givenKeyWith_stringValue() {
    String stringKey = "stringKey";
    String stringValue = "stringValue";
    jedis.set(stringKey, stringValue);
    jedis.expire(stringKey, 20);

    assertThat(jedis.persist(stringKey)).isEqualTo(1L);
    assertThat(jedis.ttl(stringKey)).isEqualTo(-1L);
  }

  @Test
  public void shouldReturnZero_givenKeyDoesNotExist() {
    assertThat(jedis.persist("doesNotExist")).isEqualTo(0L);
  }

  @Test
  public void shouldPersistKey_givenKeyWith_setValue() {
    String setKey = "setKey";
    String setMember = "setValue";

    jedis.sadd(setKey, setMember);
    jedis.expire(setKey, 20);

    assertThat(jedis.persist(setKey)).isEqualTo(1L);
    assertThat(jedis.ttl(setKey)).isEqualTo(-1L);
  }

  @Test
  public void shouldPersistKey_givenKeyWith_sortedSetValue() {
    String sortedSetKey = "sortedSetKey";
    double score = 2.0;
    String sortedSetMember = "sortedSetMember";

    jedis.zadd(sortedSetKey, score, sortedSetMember);
    jedis.expire(sortedSetKey, 20);

    assertThat(jedis.persist(sortedSetKey)).isEqualTo(1L);
    assertThat(jedis.ttl(sortedSetKey)).isEqualTo(-1L);
  }

  @Test
  public void shouldPersistKey_givenKeyWith_hashValue() {
    String hashKey = "hashKey";
    String hashField = "hashField";
    String hashValue = "hashValue";

    jedis.hset(hashKey, hashField, hashValue);
    jedis.expire(hashKey, 20);

    assertThat(jedis.persist(hashKey)).isEqualTo(1L);
    assertThat(jedis.ttl(hashKey)).isEqualTo(-1L);
  }

  @Test
  public void shouldPersistKey_givenKeyWith_geoValue() {
    String geoKey = "sicily";
    double latitude = 13.361389;
    double longitude = 38.115556;
    String geoMember = "Palermo Catania";

    jedis.geoadd(geoKey, latitude, longitude, geoMember);
    jedis.expire(geoKey, 20);

    assertThat(jedis.persist(geoKey)).isEqualTo(1L);
    assertThat(jedis.ttl(geoKey)).isEqualTo(-1L);
  }

  @Test
  public void shouldPersistKey_givenKeyWith_hyperLogLogValue() {
    String hyperLogLogKey = "crawled:127.0.0.2";
    String hyperLogLogValue = "www.insideTheHouse.com";

    jedis.pfadd(hyperLogLogKey, hyperLogLogValue);
    jedis.expire(hyperLogLogKey, 20);

    assertThat(jedis.persist(hyperLogLogKey)).isEqualTo(1L);
    assertThat(jedis.ttl(hyperLogLogKey)).isEqualTo(-1L);
  }

  @Test
  public void shouldPersistKey_givenKeyWith_listValue() {
    String listKey = "listKey";
    String listValue = "listValue";

    jedis.lpush(listKey, listValue);
    jedis.expire(listKey, 20);

    assertThat(jedis.persist(listKey)).isEqualTo(1L);
    assertThat(jedis.ttl(listKey)).isEqualTo(-1L);
  }

  @Test
  public void shouldPersistKey_givenKeyWith_bitMapValue() {
    String bitMapKey = "bitMapKey";
    long offset = 1L;
    String bitMapValue = "0";

    jedis.setbit(bitMapKey, offset, bitMapValue);
    jedis.expire(bitMapKey, 20);

    assertThat(jedis.persist(bitMapKey)).isEqualTo(1L);
    assertThat(jedis.ttl(bitMapKey)).isEqualTo(-1L);
  }

  @Test
  public void shouldPersistKeysConcurrently() throws InterruptedException {
    int iterationCount = 5000;
    setKeysWithExpiration(jedis, iterationCount);

    AtomicLong persistedFromThread1 = new AtomicLong(0);
    AtomicLong persistedFromThread2 = new AtomicLong(0);

    Runnable runnable1 = () -> persistKeys(persistedFromThread1, jedis, iterationCount);
    Runnable runnable2 = () -> persistKeys(persistedFromThread2, jedis2, iterationCount);

    Thread thread1 = new Thread(runnable1);
    Thread thread2 = new Thread(runnable2);

    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();

    assertThat(persistedFromThread1.get() + persistedFromThread2.get()).isEqualTo(iterationCount);
  }

  private void setKeysWithExpiration(Jedis jedis, int iterationCount) {
    for (int i = 0; i < iterationCount; i++) {
      SetParams setParams = new SetParams();
      setParams.ex(600);

      jedis.set("key" + i, "value" + i, setParams);
    }
  }

  private void persistKeys(AtomicLong atomicLong, Jedis jedis, int iterationCount) {
    for (int i = 0; i < iterationCount; i++) {
      String key = "key" + i;
      atomicLong.addAndGet(jedis.persist(key));
    }
  }
}
