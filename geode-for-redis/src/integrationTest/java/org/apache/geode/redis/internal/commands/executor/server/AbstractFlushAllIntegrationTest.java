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

import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractFlushAllIntegrationTest implements RedisIntegrationTest {

  protected Jedis jedis;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @Before
  public void setUp() {
    jedis = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void teardown() {
    jedis.close();
  }

  @Test
  public void givenMoreThanTwoArguments_returnsSyntaxError() {
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.FLUSHALL, "ASYNC", "extraArg"))
        .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void flushAllDeletesAllDataTypes() {
    jedis.sadd("set1", "value1");
    jedis.hset("hash1", "field1", "member1");
    jedis.set("string1", "value1");

    jedis.flushAll();

    assertThat(jedis.keys("*")).isEmpty();
    assertThat(jedis.get("string1")).isNull();
    assertThat(jedis.smembers("set1")).isEmpty();
    assertThat(jedis.hgetAll("hash1")).isEmpty();
  }
}
