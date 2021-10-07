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

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertAtLeastNArgs;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractMGetIntegrationTest implements RedisIntegrationTest {

  private JedisCluster jedis;
  private final String hashTag = "{111}";
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
  public void errors_givenWrongNumberOfArguments() {
    assertAtLeastNArgs(jedis, Protocol.Command.MGET, 1);
  }

  @Test
  public void testMGet_requestNonexistentKey_respondsWithNil() {
    String key1 = "existingKey" + hashTag;
    String key2 = "notReallyAKey" + hashTag;
    String value1 = "theRealValue" + hashTag;
    String[] keys = new String[2];
    String[] expectedVals = new String[2];
    keys[0] = key1;
    keys[1] = key2;
    expectedVals[0] = value1;
    expectedVals[1] = null;

    jedis.set(key1, value1);

    assertThat(jedis.mget(keys)).containsExactly(expectedVals);
  }

  @Test
  public void testMget_returnsNil_forNonStringKey() {
    String setKey = "set" + hashTag;
    String hashKey = "hash" + hashTag;
    String stringKey = "string" + hashTag;
    jedis.sadd(setKey, "a");
    jedis.hset(hashKey, "a", "b");
    jedis.set(stringKey, "ok");

    assertThat(jedis.mget(setKey, hashKey, stringKey))
        .containsExactly(null, null, "ok");
  }

  @Test
  public void testMget_whileConcurrentUpdates() {
    String[] keys = IntStream.range(0, 10)
        .mapToObj(x -> "key-" + x + hashTag)
        .toArray(String[]::new);

    // Should not result in any exceptions
    new ConcurrentLoopingThreads(1000,
        (i) -> jedis.set(keys[i % 10], "value-" + i),
        (i) -> jedis.mget(keys)).run();
  }
}
