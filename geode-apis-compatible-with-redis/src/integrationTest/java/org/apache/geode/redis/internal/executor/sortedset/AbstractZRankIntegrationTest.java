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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractZRankIntegrationTest implements RedisIntegrationTest {
  private JedisCluster jedis;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  private static final int ENTRY_COUNT = 10;
  private static final Random random = new Random();

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
  public void zrankErrors_givenWrongNumberOfArguments() {
    assertExactNumberOfArgs(jedis, Protocol.Command.ZSCORE, 2);
  }

  @Test
  public void zrankReturnsNil_givenNonexistentKey() {
    assertThat(jedis.zrank("fakeKey", "fakeMember")).isEqualTo(null);
  }

  @Test
  public void zrankReturnsNil_givenNonexistentMember() {
    jedis.zadd("key", 1.0, "member");
    assertThat(jedis.zrank("key", "fakeMember")).isEqualTo(null);
  }

  @Test
  public void zrankReturnsRank_givenExistingKeyAndMember() {
    jedis.zadd("key", 1.0, "member");
    assertThat(jedis.zrank("key", "member")).isEqualTo(0);
  }

  @Test
  public void zrankReturnsRankByScore_givenUniqueScoresAndMembers() {
    Map<String, Double> map = makeMemberScoreMap_differentScores();
    jedis.zadd("key", map);

    // get the ranks of the members
    Iterator<String> membersIterator = map.keySet().iterator();
    String[] members = new String[10];
    while(membersIterator.hasNext()) {
      String memberName = membersIterator.next();
      long rank = jedis.zrank("key", memberName);
      members[(int) rank] = memberName;
    }

    Double previousScore = Double.NEGATIVE_INFINITY;
    for (int i=0; i<ENTRY_COUNT; i++) {
      Double score = jedis.zscore("key", members[i]);
      assertThat(score).isGreaterThanOrEqualTo(previousScore);
      previousScore = score;
    }
  }

  private Map<String, Double> makeMemberScoreMap_differentScores() {
    Map<String, Double> map = new HashMap<>();

    // Use sets so all values are unique.
    Set<Double> scoreSet = new HashSet<>();
    while (scoreSet.size() < ENTRY_COUNT - 2) {
      double score = random.nextDouble();
      scoreSet.add(score);
    }
    scoreSet.add(Double.POSITIVE_INFINITY);
    scoreSet.add(Double.NEGATIVE_INFINITY);

    byte[] memberNameArray = new byte[6];
    Set<String> memberSet = new HashSet<>();
    while (memberSet.size() < ENTRY_COUNT) {
      random.nextBytes(memberNameArray);
      String memberName = Coder.bytesToString(memberNameArray);
      memberSet.add(memberName);
    }

    Iterator<String> memberIterator = memberSet.iterator();
    Iterator<Double> scoreIterator = scoreSet.iterator();
    while (memberIterator.hasNext()) {
      map.put(memberIterator.next(), scoreIterator.next());
    }

    return map;
  }

  public static <V, K> Map<V, K> invert(Map<K, V> map) {
    return map.entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
  }

}
