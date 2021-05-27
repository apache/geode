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

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertExactNumberOfArgs;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.RedisIntegrationTest;

public abstract class AbstractZCardIntegrationTest implements RedisIntegrationTest {

  private static final int SET_SIZE = 1000;

  private JedisCluster jedis;

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
  public void zcardTakesExactlyOneArg() {
    assertExactNumberOfArgs(jedis, Protocol.Command.ZCARD, 1);
  }

  @Test
  public void zcardReturnsZero_givenAbsentKey() {
    assertThat(jedis.zcard("absentKey")).isEqualTo(0);
  }

  @Test
  public void zcardReturnsCorrectSetSize() {
    String key = "key";
    Map<String, Double> updateMap = makeMemberScoreMap("member");
    jedis.zadd(key, updateMap);

    assertThat(jedis.zcard(key)).isEqualTo(SET_SIZE);
  }

  private Map<String, Double> makeMemberScoreMap(String baseString) {
    Map<String, Double> scoreMemberPairs = new HashMap<>();
    for (int i = 0; i < SET_SIZE; i++) {
      scoreMemberPairs.put(baseString + i, Double.valueOf(i + ""));
    }
    return scoreMemberPairs;
  }

}
