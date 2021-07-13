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
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Tuple;

import org.apache.geode.redis.RedisIntegrationTest;

@RunWith(JUnitParamsRunner.class)
public abstract class AbstractZRevRangeIntegrationTest implements RedisIntegrationTest {
  private static final String MEMBER_BASE_NAME = "member";
  private static final String KEY = "key";
  private JedisCluster jedis;
  private static final List<Double> scores =
      Arrays.asList(Double.NEGATIVE_INFINITY, -10.5, 0.0, 10.5, Double.POSITIVE_INFINITY);

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
  public void shouldError_givenNonIntegerRangeValues() {
    jedis.zadd(KEY, 1.0, MEMBER_BASE_NAME);
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.ZREVRANGE, KEY, "NOT_AN_INT", "2"))
            .hasMessageContaining(ERROR_NOT_INTEGER);
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.ZREVRANGE, KEY, "1", "NOT_AN_INT"))
            .hasMessageContaining(ERROR_NOT_INTEGER);
  }

  @Test
  public void shouldReturnSyntaxError_givenInvalidWithScoresFlag() {
    jedis.zadd(KEY, 1.0, MEMBER_BASE_NAME);
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.ZREVRANGE, KEY, "1", "2", "WITSCOREZ"))
            .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  @Parameters({"1,0", "25,30", "8,-3", "8,8", "-8,-8", "-30,-25"})
  @TestCaseName("{method}: start:{0}, end:{1}")
  public void shouldReturnEmptyCollection_givenInvalidRange(int start, int end) {
    for (int i = 0; i < scores.size(); i++) {
      String memberName = MEMBER_BASE_NAME + i;
      Double score = scores.get(i);
      jedis.zadd(KEY, score, memberName);
    }
    assertThat(jedis.zrange(KEY, start, end)).isEmpty();
  }

  @Test
  @Parameters({"0,1", "0,3", "0,6", "2,2", "2,4", "0,-2", "2,-2", "-3,-1", "-6,2"})
  @TestCaseName("{method}: start:{0}, end:{1}")
  public void zrevrange_withValidRanges(int start, int end) {
    List<String> entries = new ArrayList<>();
    for (int i = 0; i < scores.size(); i++) {
      String memberName = MEMBER_BASE_NAME + i;
      entries.add(memberName);
      Double score = scores.get(i);
      jedis.zadd(KEY, score, memberName);
    }

    validateRevrange(start, end, entries);
  }

  @Test
  @Parameters({"0,1", "0,3", "0,6", "2,2", "2,4", "0,-2", "2,-2", "-3,-1", "-6,2"})
  @TestCaseName("{method}: start:{0}, end:{1}")
  public void zrevrangeWithScores_withValidRanges(int start, int end) {
    List<Tuple> entries = new ArrayList<>();
    int numOfEntries = scores.size();
    for (int i = 0; i < numOfEntries; i++) {
      String memberName = MEMBER_BASE_NAME + i;
      Double score = scores.get(i);
      entries.add(new Tuple(memberName, score));
      jedis.zadd(KEY, score, memberName);
    }

    int subListStartIndex = getSubListStartIndex(end, numOfEntries);
    int subListEndIndex = getSubListEndIndex(start, numOfEntries);
    List<Tuple> expectedRevrange = entries.subList(subListStartIndex, subListEndIndex);
    Collections.reverse(expectedRevrange);

    Set<Tuple> revrange = jedis.zrevrangeWithScores(KEY, start, end);

    assertThat(revrange).containsExactlyElementsOf(expectedRevrange);
  }

  @Test
  @Parameters({"0,1", "0,3", "0,6", "2,2", "2,4", "0,-2", "2,-2", "-3,-1", "-6,2"})
  @TestCaseName("{method}: start:{0}, end:{1}")
  public void zrevrange_withSameScores_shouldOrderLexically(int start, int end) {
    List<String> entries = new ArrayList<>();
    for (int i = 5; i > 0; i--) {
      String memberName = MEMBER_BASE_NAME + i;
      entries.add(memberName);
      jedis.zadd(KEY, 0.1, memberName);
    }

    // Since entries were added in reverse lexicographical order, reverse the list here
    Collections.reverse(entries);

    validateRevrange(start, end, entries);
  }

  private int getSubListStartIndex(int end, int numOfEntries) {
    if (end >= 0) {
      return Math.max(numOfEntries - end - 1, 0);
    } else {
      return Math.min(-end - 1, numOfEntries - 1);
    }
  }

  private int getSubListEndIndex(int start, int numOfEntries) {
    if (start >= 0) {
      return Math.max(numOfEntries - start, 0);
    } else {
      return Math.min(-start, numOfEntries);
    }
  }

  private void validateRevrange(int start, int end, List<String> entries) {
    int subListStartIndex = getSubListStartIndex(end, entries.size());
    int subListEndIndex = getSubListEndIndex(start, entries.size());
    List<String> expectedRevrange = entries.subList(subListStartIndex, subListEndIndex);
    Collections.reverse(expectedRevrange);

    Set<String> revrange = jedis.zrevrange(KEY, start, end);

    assertThat(revrange).containsExactlyElementsOf(expectedRevrange);
  }
}
