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

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertAtLeastNArgs;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractExistsIntegrationTest implements RedisIntegrationTest {

  private JedisCluster jedis;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @Before
  public void setUp() {
    jedis = new JedisCluster(new HostAndPort("localhost", getPort()), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    flushAll();
    jedis.close();
  }

  @Test
  public void errors_givenTooFewArguments() {
    assertAtLeastNArgs(jedis, Protocol.Command.EXISTS, 1);
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
    String key1 = "{user1}key1";
    String key2 = "{user1}key2";

    jedis.set(key1, "value1");
    jedis.set(key2, "value2");

    assertThat(jedis.exists(toArray(key1, "{user1}doesNotExist1", key2, "{user1}doesNotExist2")))
        .isEqualTo(2L);
  }

  @Test
  public void shouldCorrectlyVerifyKeysExistConcurrently() {
    int iterationCount = 5000;

    new ConcurrentLoopingThreads(iterationCount, (i) -> jedis.set("key" + i, "value" + i)).run();

    AtomicLong existsCount = new AtomicLong(0);
    new ConcurrentLoopingThreads(
        iterationCount,
        (i) -> existsCount.addAndGet(jedis.exists(toArray("key" + i))),
        (i) -> existsCount.addAndGet(jedis.exists(toArray("key" + i))))
            .run();

    assertThat(existsCount.get()).isEqualTo(2 * iterationCount);
  }

  @Test
  public void shouldNotThrowExceptionsWhenConcurrentlyCreatingCheckingAndDeletingKeys() {

    int iterationCount = 5000;
    new ConcurrentLoopingThreads(
        iterationCount,
        (i) -> jedis.set("key", "value"),
        (i) -> jedis.exists(toArray("key")),
        (i) -> jedis.del("key"))
            .run();
  }

  public String[] toArray(String... strings) {
    return strings;
  }


}
