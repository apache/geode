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
import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertAtMostNArgs;
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
  private static final String nonExistentSetKey = "{user1}nonExistentSet";
  private static final String setKey = "{user1}setKey";
  private static final String[] setMembers = {"one", "two", "three", "four", "five"};

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
    assertAtLeastNArgs(jedis, Protocol.Command.SCARD, 1);
  }

  @Test
  public void srandmemberTooManyArgs_returnsError() {
    assertAtMostNArgs(jedis, Protocol.Command.SCARD, 2);
  }

  @Test
  public void srandmemberWithoutCount_withNonExistentSet_returnsNull() {
    assertThat(jedis.srandmember(nonExistentSetKey)).isNull();
    assertThat(jedis.exists(nonExistentSetKey)).isFalse();
  }

  @Test
  public void srandmemberWithoutCount_withExistentSet_returnsOneMember() {
    jedis.sadd(setKey, setMembers);

    String result = jedis.srandmember(setKey);
    assertThat(setMembers).contains(result);
  }

  @Test
  public void srandmemberWithCountAsSetSize_withExistentSet_returnsAllMembers() {
    jedis.sadd(setKey, setMembers);
    int count = setMembers.length;

    List<String> result = jedis.srandmember(setKey, count);
    assertThat(result.size()).isEqualTo(count);
    assertThat(result).containsExactlyInAnyOrder(setMembers);
  }

  @Test
  public void srandmemberWithCountAsZero_withExistentSet_returnsEmptySet() {
    assertThat(jedis.srandmember(nonExistentSetKey, 0)).isEmpty();
    assertThat(jedis.exists(nonExistentSetKey)).isFalse();
  }

  @Test
  public void srandmemberWithCountAsGreaterThanSetSize_withExistentSet_returnsAllMembersWithDuplicates() {
    jedis.sadd(setKey, setMembers);
    int count = -20;

    List<String> result = jedis.srandmember(setKey, count);
    assertThat(result.size()).isEqualTo(-count);
    for (String s : result) {
      assertThat(s).isNotNull();
      assertThat(setMembers).contains(s);
    }
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
}
