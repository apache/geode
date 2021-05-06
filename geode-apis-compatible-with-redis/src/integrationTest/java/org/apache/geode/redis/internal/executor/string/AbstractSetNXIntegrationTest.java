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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

public abstract class AbstractSetNXIntegrationTest implements RedisIntegrationTest {

  private JedisCluster jedis;
  private JedisCluster jedis2;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @Before
  public void setUp() {
    jedis = new JedisCluster(new HostAndPort("localhost", getPort()), REDIS_CLIENT_TIMEOUT);
    jedis2 = new JedisCluster(new HostAndPort("localhost", getPort()), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    flushAll();
    jedis.close();
  }

  @Test
  public void givenKeyNotProvided_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(() -> jedis.sendCommand("any", Protocol.Command.SETNX))
        .hasMessageContaining("ERR wrong number of arguments for 'setnx' command");
  }

  @Test
  public void givenValueNotProvided_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(() -> jedis.sendCommand("key", Protocol.Command.SETNX, "key"))
        .hasMessageContaining("ERR wrong number of arguments for 'setnx' command");
  }

  @Test
  public void givenMoreThanThreeArgumentsProvided_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(
        () -> jedis.sendCommand("key", Protocol.Command.SETNX, "key", "value", "extraArg"))
            .hasMessageContaining("ERR wrong number of arguments for 'setnx' command");
  }

  @Test
  public void testSetNXonNonString_doesNotThrowError() {
    jedis.sadd("set", "a");
    assertThat(jedis.setnx("set", "b")).isEqualTo(0);

    jedis.hset("hash", "a", "b");
    assertThat(jedis.setnx("hash", "b")).isEqualTo(0);
  }

  @Test
  public void testSetNX() {
    String key1 = "some-random-string";
    String key2 = "some-other-random-string";

    long response1 = jedis.setnx(key1, key1);
    long response2 = jedis.setnx(key2, key2);
    long response3 = jedis.setnx(key1, key2);

    assertThat(response1).isEqualTo(1);
    assertThat(response2).isEqualTo(1);
    assertThat(response3).isEqualTo(0);
  }

  @Test
  public void testSetNX_whenCalledConcurrently() {
    AtomicLong updateCount = new AtomicLong(0);
    int iterations = 10000;

    new ConcurrentLoopingThreads(iterations,
        (i) -> updateCount.getAndAdd(jedis.setnx("key-" + i, "value-" + i)),
        (i) -> updateCount.getAndAdd(jedis2.setnx("key-" + i, "value-" + i)))
            .runInLockstep();

    assertThat(iterations).isEqualTo(updateCount.get());
  }

}
