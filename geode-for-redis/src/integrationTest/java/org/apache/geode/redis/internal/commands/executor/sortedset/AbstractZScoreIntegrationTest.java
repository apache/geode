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
package org.apache.geode.redis.internal.commands.executor.sortedset;

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertExactNumberOfArgs;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.redis.internal.netty.Coder;

public abstract class AbstractZScoreIntegrationTest implements RedisIntegrationTest {
  private JedisCluster jedis;

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
  public void shouldError_givenWrongNumberOfArguments() {
    assertExactNumberOfArgs(jedis, Protocol.Command.ZSCORE, 2);
  }

  @Test
  public void shouldReturnNil_givenNonexistentKey() {
    assertThat(jedis.zscore("fakeKey", "fakeMember")).isEqualTo(null);
  }

  @Test
  public void shouldReturnNil_givenNonexistentMember() {
    jedis.zadd("key", 1.0, "member");
    assertThat(jedis.zscore("key", "fakeMember")).isEqualTo(null);
  }

  @Test
  public void shouldReturnScore_givenExistingKeyAndMember() {
    jedis.zadd("key", 1.0, "member");
    assertThat(jedis.zscore("key", "member")).isEqualTo(1.0);
  }

  @Test
  public void shouldReturnScoreWithSamePrecision() {
    String key = "key";
    String member = "member";
    double score = 0.6792199922163132d;
    byte[] byteScore = "0.6792199922163132".getBytes();

    jedis.zadd(key, score, member);

    double retScore = jedis.zscore(key, member);
    assertThat(retScore).isEqualTo(score);

    assertThat(Coder.doubleToBytes(retScore)).isEqualTo(byteScore);
  }

  @Test
  public void shouldReturnScoreWithCloseToSamePrecision_givenTooPreciseAScore() {
    String key = "key";
    String member = "member";
    double score = 0.6792199922163132456d;
    byte[] byteScore = "0.6792199922163132".getBytes();

    jedis.zadd(key, score, member);

    double retScore = jedis.zscore(key, member);
    assertThat(retScore).isEqualTo(score);

    assertThat(Coder.doubleToBytes(retScore)).isEqualTo(byteScore);
  }
}
