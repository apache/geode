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
 *
 */

package org.apache.geode.redis.internal.commands.executor.server;

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertExactNumberOfArgs;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractTimeIntegrationTest implements RedisIntegrationTest {

  private Jedis jedis;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @Before
  public void setUp() {
    jedis = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void classLevelTearDown() {
    jedis.close();
  }

  @Test
  public void givenMoreThanOneArgument_returnsWrongNumberOfArgumentsError() {
    assertExactNumberOfArgs(jedis, Protocol.Command.TIME, 0);
  }

  @Test
  public void timeCommandRespondsWithTwoValues() {
    List<String> timestamp = jedis.time();

    assertThat(timestamp).hasSize(2);
    assertThat(Long.parseLong(timestamp.get(0))).isGreaterThan(0);
    assertThat(Long.parseLong(timestamp.get(1))).isNotNegative();
  }
  //
  // @Test
  // public void addAlotOfKeysToNativeRedis() {
  // for(int i=0; i < 1000000; i++) {
  // jedis.set(String.valueOf(i), "whatevs");
  // }
  //
  // jedis.sendCommand(Protocol.Command.FLUSHALL, "ASYNC");
  // Set<String> keys = jedis.keys("*");
  //
  // assertThat(keys).isEmpty();
  // }
}
