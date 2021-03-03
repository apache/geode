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
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.RedisPortSupplier;

public abstract class AbstractMGetIntegrationTest implements RedisPortSupplier {

  private Jedis jedis;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @Before
  public void setUp() {
    jedis = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    jedis.flushAll();
    jedis.close();
  }

  @Test
  public void errors_givenWrongNumberOfArguments() {
    assertAtLeastNArgs(jedis, Protocol.Command.MGET, 1);
  }

  @Test
  public void testMGet_requestNonexistentKey_respondsWithNil() {
    String key1 = "existingKey";
    String key2 = "notReallyAKey";
    String value1 = "theRealValue";
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
    jedis.sadd("set", "a");
    jedis.hset("hash", "a", "b");
    jedis.set("string", "ok");

    assertThat(jedis.mget("set", "hash", "string"))
        .containsExactly(null, null, "ok");
  }

  @Test
  public void testMget_whileConcurrentUpdates() {
    Jedis jedis2 = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
    String[] keys = IntStream.range(0, 10)
        .mapToObj(x -> "key-" + x)
        .toArray(String[]::new);

    // Should not result in any exceptions
    new ConcurrentLoopingThreads(1000,
        (i) -> jedis.set(keys[i % 10], "value-" + i),
        (i) -> jedis2.mget(keys)).run();

    jedis2.close();
  }
}
