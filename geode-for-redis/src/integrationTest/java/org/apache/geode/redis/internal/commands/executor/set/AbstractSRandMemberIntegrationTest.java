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
package org.apache.geode.redis.internal.commands.executor.set;

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertAtLeastNArgs;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_TYPE;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.RedisIntegrationTest;

public abstract class AbstractSRandMemberIntegrationTest implements RedisIntegrationTest {
  private JedisCluster jedis;
  private static final String NON_EXISTENT_SET_KEY = "{user1}nonExistentSet";
  private static final String SET_KEY = "{user1}setKey";
  private static final String[] SET_MEMBERS = {"one", "two", "three", "four", "five"};

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
  public void srandmemberTooFewArgs_returnsError() {
    assertAtLeastNArgs(jedis, Protocol.Command.SRANDMEMBER, 1);
  }

  @Test
  public void srandmemberTooManyArgs_returnsError() {
    assertThatThrownBy(
        () -> jedis.sendCommand(SET_KEY, Protocol.Command.SRANDMEMBER, SET_KEY, "5", "5"))
            .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void srandmemberWithInvalidCount_returnsError() {
    assertThatThrownBy(() -> jedis.sendCommand(SET_KEY, Protocol.Command.SRANDMEMBER, SET_KEY, "b"))
        .hasMessageContaining(ERROR_NOT_INTEGER);
  }

  @Test
  public void srandmemberWithoutCount_withNonExistentSet_returnsNull() {
    assertThat(jedis.srandmember(NON_EXISTENT_SET_KEY)).isNull();
    assertThat(jedis.exists(NON_EXISTENT_SET_KEY)).isFalse();
  }

  @Test
  public void srandmemberWithCount_withNonExistentSet_returnsEmptySet() {
    assertThat(jedis.srandmember(NON_EXISTENT_SET_KEY, 1)).isEmpty();
    assertThat(jedis.exists(NON_EXISTENT_SET_KEY)).isFalse();
  }

  @Test
  public void srandmemberWithoutCount_withExistentSet_returnsOneMember() {
    jedis.sadd(SET_KEY, SET_MEMBERS);

    String result = jedis.srandmember(SET_KEY);
    assertThat(SET_MEMBERS).contains(result);
  }

  @Test
  public void srandmemberWithCount_withExistentSet_returnsCorrectNumberOfMembers() {
    jedis.sadd(SET_KEY, SET_MEMBERS);
    int count = 2;

    List<String> result = jedis.srandmember(SET_KEY, count);
    assertThat(result.size()).isEqualTo(2);
    assertThat(result).isSubsetOf(SET_MEMBERS);
    assertThat(result).doesNotHaveDuplicates();
  }

  @Test
  public void srandmemberWithCountAsSetSize_withExistentSet_returnsAllMembers() {
    jedis.sadd(SET_KEY, SET_MEMBERS);
    int count = SET_MEMBERS.length;

    assertThat(jedis.srandmember(SET_KEY, count)).containsExactlyInAnyOrder(SET_MEMBERS);
  }

  @Test
  public void srandmemberWithNegativeCount_withExistentSet_returnsSubsetOfSet() {
    jedis.sadd(SET_KEY, SET_MEMBERS);
    int count = -20;

    List<String> result = jedis.srandmember(SET_KEY, count);
    assertThat(result.size()).isEqualTo(-count);
    assertThat(result).isSubsetOf(SET_MEMBERS);
  }

  @Test
  public void srandmemberWithCountGreaterThanSet_withExistentSet_returnsAllMembers() {
    jedis.sadd(SET_KEY, SET_MEMBERS);
    int count = 20;

    assertThat(jedis.srandmember(SET_KEY, count)).containsExactlyInAnyOrder(SET_MEMBERS);
  }

  @Test
  public void srandmemberWithoutCount_withWrongKeyType_returnsWrongTypeError() {
    String key = "ding";
    jedis.set(key, "dong");
    assertThatThrownBy(() -> jedis.srandmember(key)).hasMessageContaining(ERROR_WRONG_TYPE);
  }

  @Test
  public void srandmemberWithCount_withWrongKeyType_returnsWrongTypeError() {
    String key = "ding";
    jedis.set(key, "dong");
    assertThatThrownBy(() -> jedis.srandmember(key, 5)).hasMessageContaining(ERROR_WRONG_TYPE);
  }

  @Test
  public void srandmemberWithCount_countAsZero_withWrongKeyType_returnsWrongTypeError() {
    String key = "ding";
    jedis.set(key, "dong");
    assertThatThrownBy(() -> jedis.srandmember(key, 0)).hasMessageContaining(ERROR_WRONG_TYPE);
  }
}
