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

import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.exceptions.JedisClusterMaxAttemptsException;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class ZRemRangeByLexDUnitTest {
  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  @Rule
  public RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(4);

  private JedisCluster jedis1;
  private JedisCluster jedis2;
  private List<MemberVM> servers;
  private static final String sortedSetKey = "key";
  private final int setSize = 500;

  @Before
  public void setup() {
    MemberVM locator = clusterStartUp.startLocatorVM(0);
    int locatorPort = locator.getPort();
    MemberVM server1 = clusterStartUp.startRedisVM(1, locatorPort);
    MemberVM server2 = clusterStartUp.startRedisVM(2, locatorPort);
    servers = new ArrayList<>();
    servers.add(server1);
    servers.add(server2);

    int redisServerPort1 = clusterStartUp.getRedisPort(1);
    int redisServerPort2 = clusterStartUp.getRedisPort(2);

    jedis1 =
        new JedisCluster(new HostAndPort(BIND_ADDRESS, redisServerPort1), REDIS_CLIENT_TIMEOUT);
    jedis2 =
        new JedisCluster(new HostAndPort(BIND_ADDRESS, redisServerPort2), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    jedis1.close();
  }

  @Test
  public void ZRemRangeByLex_removesMembersInRangeFromBothServers() {
    Map<String, Double> memberScoreMap = makeMemberScoreMap(setSize);
    jedis1.zadd(sortedSetKey, memberScoreMap);
    verifyDataExists(memberScoreMap);

    List<String> expected = new ArrayList<>(memberScoreMap.keySet().stream().sorted()
        .collect(Collectors.toList())).subList(4, setSize);

    assertThat(jedis1.zrank(sortedSetKey, "vvvvv")).isEqualTo(4L);
    assertThat(jedis2.zrank(sortedSetKey, "vvvvv")).isEqualTo(4L);

    assertThat(jedis1.zremrangeByLex(sortedSetKey, "[" + "v", "[" + "vvvv")).isEqualTo(4);

    assertThat(jedis1.zrank(sortedSetKey, "vvvvv")).isEqualTo(0L);
    assertThat(jedis2.zrank(sortedSetKey, "vvvvv")).isEqualTo(0L);

    assertThat(jedis1.zrange(sortedSetKey, 0, -1)).hasSameElementsAs(expected);
    assertThat(jedis2.zrange(sortedSetKey, 0, -1)).hasSameElementsAs(expected);
  }

  @Test
  public void ZRemRangeByLex_concurrentlyRemovesMembersInRangeFromBothServers() {
    Map<String, Double> memberScoreMap = makeMemberScoreMap(setSize);
    jedis1.zadd(sortedSetKey, memberScoreMap);
    verifyDataExists(memberScoreMap);

    assertThat(jedis1.zrank(sortedSetKey, "vvvvv")).isEqualTo(4L);
    assertThat(jedis2.zrank(sortedSetKey, "vvvvv")).isEqualTo(4L);

    AtomicInteger totalRemoved = new AtomicInteger();
    new ConcurrentLoopingThreads(setSize,
        (i) -> doZRemRangeByLexOnMembers(i, totalRemoved),
        (i) -> doZRemRangeByLexOnMembersInDifferentOrder(i, totalRemoved)).run();

    assertThat(jedis1.exists(sortedSetKey)).isFalse();
    assertThat(jedis2.exists(sortedSetKey)).isFalse();

    assertThat(totalRemoved.get()).isEqualTo(setSize);
  }

  @Test
  public void zRemRangeByLexRemovesMembersFromSortedSetAfterPrimaryShutsDown() {
    Map<String, Double> memberScoreMap = makeMemberScoreMap(setSize);
    jedis1.zadd(sortedSetKey, memberScoreMap);
    verifyDataExists(memberScoreMap);

    stopNodeWithPrimaryBucketOfTheKey(false);

    doZRemRangeByLexWithRetries(memberScoreMap);

    verifyDataDoesNotExist(memberScoreMap);
    assertThat(jedis1.exists(sortedSetKey)).isFalse();
    assertThat(jedis2.exists(sortedSetKey)).isFalse();
  }

  @Test
  @Ignore("Fails due to GEODE-9310")
  // TODO this test is not fully converted from the ZRem one to the ZRemRangeByLex one
  // come back to this when GEODE-9310 is resolved.
  public void zRemRangeByLexCanRemoveMembersFromSortedSetWhenPrimaryIsCrashed() throws Exception {
    int mapSize = 300;
    Map<String, Double> memberScoreMap = makeMemberScoreMap(mapSize);

    jedis1.zadd(sortedSetKey, memberScoreMap);
    verifyDataExists(memberScoreMap);

    int number = 10;
    String memberNotRemoved = buildMemberName(number);
    List<String> membersToRemove = new ArrayList<>(memberScoreMap.keySet());
    membersToRemove.remove(memberNotRemoved);

    Future<Void> future1 = executor.submit(() -> doZRemRangeByLex(memberScoreMap));
    Future<Void> future2 = executor.submit(() -> stopNodeWithPrimaryBucketOfTheKey(true));

    future1.get();
    future2.get();

    GeodeAwaitility.await().until(() -> verifyDataDoesNotExist(memberScoreMap));
    assertThat(jedis1.exists(sortedSetKey)).isTrue();
  }

  private void doZRemRangeByLex(Map<String, Double> membersMap) {

  }

  private void stopNodeWithPrimaryBucketOfTheKey(boolean isCrash) {
    boolean isPrimary;
    for (MemberVM server : servers) {
      isPrimary = server.invoke(ZRemRangeByLexDUnitTest::isPrimaryForKey);
      if (isPrimary) {
        if (isCrash) {
          server.getVM().bounceForcibly();
        } else {
          server.stop();
        }
        return;
      }
    }
  }

  private static boolean isPrimaryForKey() {
    int bucketId = getBucketId(new RedisKey(sortedSetKey.getBytes()));
    return isPrimaryForBucket(bucketId);
  }

  private static int getBucketId(Object key) {
    return PartitionedRegionHelper.getHashKey((PartitionedRegion) getDataRegion(), Operation.GET,
        key, null, null);
  }

  private static Region<RedisKey, RedisData> getDataRegion() {
    return ClusterStartupRule.getCache().getRegion(RegionProvider.REDIS_DATA_REGION);
  }

  private static boolean isPrimaryForBucket(int bucketId) {
    return ((PartitionedRegion) getDataRegion()).getLocalPrimaryBucketsListTestOnly()
        .contains(bucketId);
  }

  private Map<String, Double> makeMemberScoreMap(int setSize) {
    Map<String, Double> scoreMemberPairs = new HashMap<>();
    String memberName = "";
    for (int i = 0; i < setSize; i++) {
      memberName = memberName.concat("v");
      scoreMemberPairs.put(memberName, 5.0);
    }
    return scoreMemberPairs;
  }

  private void doZRemRangeByLexWithRetries(Map<String, Double> map) {
    int maxRetryAttempts = 10;
    int retryAttempts = 0;
    while (!zRemRangeByLexWithRetries(map, retryAttempts, maxRetryAttempts)) {
      retryAttempts++;
    }
  }

  private boolean zRemRangeByLexWithRetries(Map<String, Double> map, int retries, int maxRetries) {
    long removed;
    try {
      removed = jedis1.zremrangeByLex(sortedSetKey, "[v", "[w");
    } catch (JedisClusterMaxAttemptsException e) {
      if (retries < maxRetries) {
        return false;
      }
      throw e;
    }
    assertThat(removed).isEqualTo(map.size());
    return true;
  }

  private void doZRemRangeByLexOnMembers(int i, AtomicInteger total) {
    String memberName = buildMemberName(i);
    long count = jedis1.zremrangeByLex(sortedSetKey, "[" + memberName, "[" + memberName);
    total.addAndGet((int) count);
  }

  private void doZRemRangeByLexOnMembersInDifferentOrder(int i, AtomicInteger total) {
    String memberName = buildMemberName(setSize - i);
    long count = jedis1.zremrangeByLex(sortedSetKey, "[" + memberName, "[" + memberName);
    total.addAndGet((int) count);
  }

  private String buildMemberName(int iteration) {
    String memberName = "";
    for (int i = 0; i < iteration; i++) {
      memberName = memberName.concat("v");
    }

    return memberName;
  }

  private void verifyDataExists(Map<String, Double> memberScoreMap) {
    for (String member : memberScoreMap.keySet()) {
      Double score = jedis1.zscore(sortedSetKey, member);
      assertThat(score).isEqualTo(memberScoreMap.get(member));
    }
  }

  private boolean verifyDataDoesNotExist(Map<String, Double> memberScoreMap) {
    try {
      for (String member : memberScoreMap.keySet()) {
        Double score = jedis1.zscore(sortedSetKey, member);
        assertThat(score).isNull();
      }
    } catch (JedisClusterMaxAttemptsException e) {
      return false;
    }
    return true;
  }
}
