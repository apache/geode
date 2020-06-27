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

package org.apache.geode.redis.internal.executor.key;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

import org.apache.geode.redis.GeodeRedisServerRule;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public class PersistIntegrationTest {

  public static Jedis jedis;
  public static Jedis jedis2;
  public static int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @ClassRule
  public static GeodeRedisServerRule server = new GeodeRedisServerRule();

  @BeforeClass
  public static void setUp() {
    jedis = new Jedis("localhost", server.getPort(), REDIS_CLIENT_TIMEOUT);
    jedis2 = new Jedis("localhost", server.getPort(), REDIS_CLIENT_TIMEOUT);
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
