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
package org.apache.geode.redis.internal.executor.sortedset;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.redis.internal.RedisConstants;

public abstract class AbstractZRangeIntegrationTest implements RedisIntegrationTest {
  private JedisCluster jedis;
  private static final String SORTED_SET_KEY = "ss_key";

  @Before
  public void setUp() {
    jedis = new JedisCluster(new HostAndPort(BIND_ADDRESS, getPort()), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    flushAll();
    jedis.close();
  }

  @Test
  public void shouldError_givenWrongKeyType() {
    final String STRING_KEY = "stringKey";
    jedis.set(STRING_KEY, "value");
    assertThatThrownBy(
        () -> jedis.sendCommand(STRING_KEY, Protocol.Command.ZRANGE, STRING_KEY, "1", "2"))
            .hasMessage("WRONGTYPE " + RedisConstants.ERROR_WRONG_TYPE);
  }

  @Test
  public void shouldError_givenNonIntegerRangeValues() {
    jedis.zadd(SORTED_SET_KEY, 1.0, "member");
    assertThatThrownBy(
        () -> jedis.sendCommand(SORTED_SET_KEY, Protocol.Command.ZRANGE, SORTED_SET_KEY,
            "NOT_AN_INT", "2"))
                .hasMessageContaining(ERROR_NOT_INTEGER);
    assertThatThrownBy(
        () -> jedis.sendCommand(SORTED_SET_KEY, Protocol.Command.ZRANGE, SORTED_SET_KEY, "1",
            "ALSO_NOT_AN_INT"))
                .hasMessageContaining(ERROR_NOT_INTEGER);
  }

  @Test
  public void shouldReturnSyntaxError_givenWrongWithscoresFlag() {
    jedis.zadd(SORTED_SET_KEY, 1.0, "member");
    assertThatThrownBy(
        () -> jedis.sendCommand(SORTED_SET_KEY, Protocol.Command.ZRANGE, SORTED_SET_KEY, "1", "2",
            "WITSCOREZ"))
                .hasMessageContaining(ERROR_SYNTAX);
  }


}
