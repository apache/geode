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
package org.apache.geode.redis.internal.commands.executor.string;

import static java.lang.Integer.parseInt;
import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertExactNumberOfArgs;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisDataException;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.redis.internal.RedisConstants;

public abstract class AbstractGetSetIntegrationTest implements RedisIntegrationTest {

  private JedisCluster jedis;
  private static final int ITERATION_COUNT = 4000;

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
  public void errors_givenWrongNumberOfParameters() {
    assertExactNumberOfArgs(jedis, Protocol.Command.GETSET, 2);
  }

  @Test
  public void testGetSet_updatesKeyWithNewValue_returnsOldValue() {
    String key = randString();
    String contents = randString();
    jedis.set(key, contents);

    String newContents = randString();
    String oldContents = jedis.getSet(key, newContents);
    assertThat(oldContents).isEqualTo(contents);

    contents = newContents;
    newContents = jedis.get(key);
    assertThat(newContents).isEqualTo(contents);
  }

  @Test
  public void testGetSet_setsNonexistentKeyToNewValue_returnsNull() {
    String key = randString();
    String newContents = randString();

    String oldContents = jedis.getSet(key, newContents);
    assertThat(oldContents).isNull();

    String contents = jedis.get(key);
    assertThat(newContents).isEqualTo(contents);
  }

  @Test
  public void testGetSet_shouldWorkWith_INCR_Command() {
    String key = "key";
    Long resultLong;
    String resultString;

    jedis.set(key, "0");

    resultLong = jedis.incr(key);
    assertThat(resultLong).isEqualTo(1);

    resultString = jedis.getSet(key, "0");
    assertThat(parseInt(resultString)).isEqualTo(1);

    resultString = jedis.get(key);
    assertThat(parseInt(resultString)).isEqualTo(0);

    resultLong = jedis.incr(key);
    assertThat(resultLong).isEqualTo(1);
  }

  @Test
  public void testGetSet_whenWrongType_shouldReturnError() {
    String key = "key";
    jedis.hset(key, "field", "some hash value");

    assertThatThrownBy(() -> jedis.getSet(key, "this value doesn't matter"))
        .isInstanceOf(JedisDataException.class)
        .hasMessageContaining(RedisConstants.ERROR_WRONG_TYPE);
  }

  @Test
  public void testGetSet_shouldBeAtomic() {
    String contestedKey = "contestedKey";
    jedis.set(contestedKey, "0");
    assertThat(jedis.get(contestedKey)).isEqualTo("0");

    final AtomicInteger sumHolder = new AtomicInteger(0);
    new ConcurrentLoopingThreads(ITERATION_COUNT,
        (ignored) -> jedis.incr(contestedKey),
        (i) -> {
          // continue fetching with getSet until caught up with incrementing
          // noinspection StatementWithEmptyBody
          while (sumHolder.getAndAdd(Integer.parseInt(jedis.getSet(contestedKey, "0"))) <= i);
        }).run();
    assertThat(sumHolder.get()).isEqualTo(ITERATION_COUNT);
  }

  private String randString() {
    return Long.toHexString(Double.doubleToLongBits(Math.random()));
  }
}
