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
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.redis.internal.data.RedisSortedSet;

public abstract class AbstractZRankIntegrationTest implements RedisIntegrationTest {
  public static final String KEY = "key";
  public static final String MEMBER = "member";
  private JedisCluster jedis;

  private static final int ENTRY_COUNT = 10;
  private static final Random random = new Random();

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
    assertExactNumberOfArgs(jedis, Protocol.Command.ZRANK, 2);
  }

  @Test
  public void shouldReturnNil_givenNonexistentKey() {
    assertThat(jedis.zrank("fakeKey", "fakeMember")).isEqualTo(null);
  }

  @Test
  public void shouldReturnNil_givenNonexistentMember() {
    jedis.zadd(KEY, 1.0, MEMBER);
    assertThat(jedis.zrank(KEY, "fakeMember")).isEqualTo(null);
  }

  @Test
  public void shouldReturnRank_givenExistingKeyAndMember() {
    jedis.zadd(KEY, 1.0, MEMBER);
    assertThat(jedis.zrank(KEY, MEMBER)).isEqualTo(0);
  }

  @Test
  public void shouldReturnRankByScore_givenUniqueScoresAndMembers() {
    Map<String, Double> map = makeMemberScoreMap_differentScores();
    jedis.zadd(KEY, map);

    // get the ranks of the members
    String[] members = new String[ENTRY_COUNT];
    for (String memberName : map.keySet()) {
      long rank = jedis.zrank(KEY, memberName);
      members[(int) rank] = memberName;
    }

    Double previousScore = Double.NEGATIVE_INFINITY;
    for (String member : members) {
      Double score = jedis.zscore(KEY, member);
      assertThat(score).isGreaterThanOrEqualTo(previousScore);
      previousScore = score;
    }
  }

  @Test
  public void shouldReturnRankByLex_givenMembersWithSameScore() {
    Map<String, Double> map = makeMemberScoreMap_sameScores();
    jedis.zadd(KEY, map);

    // get the ranks of the members
    Map<Long, byte[]> rankMap = new HashMap<>();
    List<byte[]> memberList = new ArrayList<>();
    for (String memberName : map.keySet()) {
      long rank = jedis.zrank(KEY, memberName);
      rankMap.put(rank, memberName.getBytes());
      memberList.add(memberName.getBytes());
    }

    memberList.sort(new ByteArrayComparator());

    assertThat(rankMap.values()).containsExactlyElementsOf(memberList);
  }

  @Test
  public void shouldUpdateRank_whenScoreChanges() {
    Map<String, Double> memberScoreMap = new HashMap<>();

    memberScoreMap.put("first", 2.0);
    memberScoreMap.put("second", 3.0);
    memberScoreMap.put("third", 4.0);

    jedis.zadd(KEY, memberScoreMap);
    assertThat(jedis.zrank(KEY, "first")).isEqualTo(0);
    assertThat(jedis.zrank(KEY, "second")).isEqualTo(1);
    assertThat(jedis.zrank(KEY, "third")).isEqualTo(2);

    jedis.zadd(KEY, 1.0, MEMBER);
    assertThat(jedis.zrank(KEY, MEMBER)).isEqualTo(0);
    assertThat(jedis.zrank(KEY, "first")).isEqualTo(1);
  }

  @Test
  public void shouldSort_byBothScoreAndLexical() {
    Map<String, Double> memberScoreMap = makeMemberScoreMap_withVariedScores();
    Map<Long, String> rankMap = makeRankToMemberMap_withExpectedRankings();

    jedis.zadd(KEY, memberScoreMap);

    for (long i = 0; i < rankMap.size(); i++) {
      assertThat(i).isEqualTo(jedis.zrank(KEY, rankMap.get(i)));
    }

  }

  private Map<String, Double> makeMemberScoreMap_withVariedScores() {
    Map<String, Double> map = new HashMap<>();

    map.put("equal-a", 1.0);
    map.put("equal-b", 1.0);
    map.put("posInfy", Double.POSITIVE_INFINITY);
    map.put("negInfy", Double.NEGATIVE_INFINITY);
    map.put("minus", -1000.5);
    map.put("maxy", Double.MAX_VALUE);
    map.put("minnie", Double.MIN_VALUE);
    map.put("zed", 0.0);
    map.put("zero", 0.0);
    map.put("century", 100.0);

    return map;
  }

  private Map<Long, String> makeRankToMemberMap_withExpectedRankings() {
    Map<Long, String> map = new HashMap<>();

    map.put(0L, "negInfy");
    map.put(1L, "minus");
    map.put(2L, "zed");
    map.put(3L, "zero");
    map.put(4L, "minnie");
    map.put(5L, "equal-a");
    map.put(6L, "equal-b");
    map.put(7L, "century");
    map.put(8L, "maxy");
    map.put(9L, "posInfy");

    return map;
  }

  private static class ByteArrayComparator implements Comparator<byte[]> {
    @Override
    public int compare(byte[] o1, byte[] o2) {
      return RedisSortedSet.javaImplementationOfAnsiCMemCmp(o1, o2);
    }
  }

  private Map<String, Double> makeMemberScoreMap_sameScores() {
    Map<String, Double> map = new HashMap<>();

    // Use set so all values are unique.
    Set<String> memberSet = initializeSetWithRandomMemberValues();

    for (String s : memberSet) {
      map.put(s, 1.0);
    }

    return map;
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

    Set<String> memberSet = initializeSetWithRandomMemberValues();

    Iterator<String> memberIterator = memberSet.iterator();
    Iterator<Double> scoreIterator = scoreSet.iterator();
    while (memberIterator.hasNext()) {
      map.put(memberIterator.next(), scoreIterator.next());
    }

    return map;
  }

  private Set<String> initializeSetWithRandomMemberValues() {
    int leftLimit = 32; // first non-control UTF-8 character (space)
    int rightLimit = 126; // last non-control UTF-8 character (~) before range would include control
                          // characters
    Set<String> memberSet = new HashSet<>();
    while (memberSet.size() < ENTRY_COUNT) {
      String memberName = random.ints(leftLimit, rightLimit + 1)
          .limit(6)
          .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
          .toString();
      memberSet.add(memberName);
    }
    return memberSet;
  }

}
