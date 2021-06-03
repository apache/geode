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

import java.nio.charset.StandardCharsets;
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
  public void shouldError_givenWrongNumberOfArguments() {
    assertExactNumberOfArgs(jedis, Protocol.Command.ZSCORE, 2);
  }

  @Test
  public void shouldReturnNil_givenNonexistentKey() {
    assertThat(jedis.zrank("fakeKey", "fakeMember")).isEqualTo(null);
  }

  @Test
  public void shouldReturnNil_givenNonexistentMember() {
    jedis.zadd("key", 1.0, "member");
    assertThat(jedis.zrank("key", "fakeMember")).isEqualTo(null);
  }

  @Test
  public void shouldReturnRank_givenExistingKeyAndMember() {
    jedis.zadd("key", 1.0, "member");
    assertThat(jedis.zrank("key", "member")).isEqualTo(0);
  }

  @Test
  public void shouldReturnRankByScore_givenUniqueScoresAndMembers() {
    Map<String, Double> map = makeMemberScoreMap_differentScores();
    jedis.zadd("key", map);

    // get the ranks of the members
    Iterator<String> membersIterator = map.keySet().iterator();
    String[] members = new String[10];
    while (membersIterator.hasNext()) {
      String memberName = membersIterator.next();
      long rank = jedis.zrank("key", memberName);
      members[(int) rank] = memberName;
    }

    Double previousScore = Double.NEGATIVE_INFINITY;
    for (int i = 0; i < ENTRY_COUNT; i++) {
      Double score = jedis.zscore("key", members[i]);
      assertThat(score).isGreaterThanOrEqualTo(previousScore);
      previousScore = score;
    }
  }

  @Test
  public void shouldReturnRankByLex_givenMembersWithSameScore() {
    Map<String, Double> map = makeMemberScoreMap_sameScores();
    jedis.zadd("key", map);

    // get the ranks of the members
    Iterator<String> membersIterator = map.keySet().iterator();
    Map<Long, byte[]> rankMap = new HashMap<>();
    List<byte[]> memberList = new ArrayList<>();
    while (membersIterator.hasNext()) {
      String memberName = membersIterator.next();
      long rank = jedis.zrank("key", memberName);
      rankMap.put(rank, memberName.getBytes(StandardCharsets.UTF_8));
      memberList.add(memberName.getBytes(StandardCharsets.UTF_8));
    }

    memberList.sort(new byteArrayComparator());

    for (int i = 0; i < 10; i++) {
      assertThat(rankMap.get((long) i)).isEqualTo(memberList.get(i));
    }
  }

  @Test
  public void shouldSort_byBothScoreAndLexical() {
    Map<String, Double> memberScoreMap = makeMemberScoreMap_withVariedScores_andADuplicate();
    Map<Long, String> rankMap = makeRankToMemberMap_withExpectedRankings();

    jedis.zadd("key", memberScoreMap);

    for (long i = 0; i < rankMap.size(); i++) {
      assertThat(i).isEqualTo(jedis.zrank("key", rankMap.get(i)));
    }
  }

  private Map<String, Double> makeMemberScoreMap_withVariedScores_andADuplicate() {
    Map<String, Double> map = new HashMap<>();

    map.put("equal-a", 1.0);
    map.put("equal-b", 1.0);
    map.put("changed", 0.0);
    map.put("posInfy", Double.POSITIVE_INFINITY);
    map.put("negInfy", Double.NEGATIVE_INFINITY);
    map.put("minus", -1000.5);
    map.put("maxy", Double.MAX_VALUE);
    map.put("minnie", Double.MIN_VALUE);
    map.put("zed", 0.0);
    map.put("zero", 0.0);
    map.put("changed", 100.0);

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
    map.put(7L, "changed");
    map.put(8L, "maxy");
    map.put(9L, "posInfy");

    return map;
  }

  private static class byteArrayComparator implements Comparator<byte[]> {
    @Override
    public int compare(byte[] o1, byte[] o2) {
      int last = Math.min(o1.length, o2.length);
      for (int i = 0; i < last; i++) {
        int localComp = Byte.toUnsignedInt(o1[i]) - Byte.toUnsignedInt(o2[i]);
        if (localComp != 0) {
          return localComp;
        }
      }
      // shorter array whose items are all equal to the first items of a longer array is
      // considered 'less than'
      if (o1.length < o2.length) {
        return -1; // o1 < o2
      } else if (o1.length > o2.length) {
        return 1; // o2 < o1
      }
      return 0; // totally equal - should never happen...
    }
  }

  private Map<String, Double> makeMemberScoreMap_sameScores() {
    Map<String, Double> map = new HashMap<>();

    // Use set so all values are unique.
    byte[] memberNameArray = new byte[6];
    Set<String> memberSet = new HashSet<>();
    while (memberSet.size() < ENTRY_COUNT) {
      random.nextBytes(memberNameArray);
      String memberName = Coder.bytesToString(memberNameArray);
      memberSet.add(memberName);
    }

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
}
