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
package org.apache.geode.redis.internal.executor.string;

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertExactNumberOfArgs;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractIncrByIntegrationTest implements RedisIntegrationTest {

  private JedisCluster jedis1;
  private Random rand;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @Before
  public void setUp() {
    rand = new Random();

    jedis1 = new JedisCluster(new HostAndPort("localhost", getPort()), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    flushAll();
    jedis1.close();
  }

  @Test
  public void errors_givenWrongNumberOfArguments() {
    assertExactNumberOfArgs(jedis1, Protocol.Command.INCRBY, 2);
  }

  @Test
  public void testIncrBy_failsWhenPerformedOnNonIntegerValue() {
    String key = "key";
    jedis1.sadd(key, "member");
    assertThatThrownBy(() -> jedis1.incrBy(key, 1))
        .hasMessageContaining("WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  @Test
  public void testIncrBy_createsAndIncrementsNonExistentKey() {
    assertThat(jedis1.incrBy("nonexistentkey", 1)).isEqualTo(1);
    assertThat(jedis1.incrBy("otherNonexistentKey", -1)).isEqualTo(-1);
  }

  @Test
  public void incrBy_incrementsPositiveIntegerValue() {
    String key = "key";
    int num = 100;
    int increment = rand.nextInt(100);

    jedis1.set(key, String.valueOf(num));
    jedis1.incrBy(key, increment);
    assertThat(jedis1.get(key)).isEqualTo(String.valueOf(num + increment));
  }

  @Test
  public void incrBy_incrementsNegativeValue() {
    String key = "key";
    int num = -100;
    int increment = rand.nextInt(100);

    jedis1.set(key, "" + num);
    jedis1.incrBy(key, increment);
    assertThat(jedis1.get(key)).isEqualTo(String.valueOf(num + increment));
  }

  @Test
  public void testIncrBy_IncrementingMaxValueThrowsError() {
    String key = "key";
    Long increment = Long.MAX_VALUE / 2;

    jedis1.set(key, String.valueOf(Long.MAX_VALUE));
    assertThatThrownBy(() -> jedis1.incrBy(key, increment))
        .hasMessageContaining("ERR increment or decrement would overflow");
  }

  @Test
  public void testConcurrentIncrBy_performsAllIncrBys() {
    String key = "key";
    AtomicInteger expectedValue = new AtomicInteger(0);

    jedis1.set(key, "0");

    new ConcurrentLoopingThreads(1000,
        (i) -> {
          int increment = ThreadLocalRandom.current().nextInt(-50, 50);
          expectedValue.addAndGet(increment);
          jedis1.incrBy(key, increment);
        },
        (i) -> {
          int increment = ThreadLocalRandom.current().nextInt(-50, 50);
          expectedValue.addAndGet(increment);
          jedis1.incrBy(key, increment);
        }).run();

    assertThat(Integer.parseInt(jedis1.get(key))).isEqualTo(expectedValue.get());
  }

  @Test
  public void testIncrByErrorsForValuesGreaterThatMaxInt() {
    jedis1.set("key", "9223372036854775808");

    assertThatThrownBy(() -> jedis1.incrBy("key", 1)).hasMessageContaining(ERROR_NOT_INTEGER);
  }

}
