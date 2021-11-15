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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
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
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class ZRemRangeByLexDUnitTest {
  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  @Rule
  public RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(4);

  private JedisCluster jedis;
  private List<MemberVM> servers;
  private static final String KEY = "key";
  public static final String MEMBER_BASE_STRING = "v";
  private final int SET_SIZE = 500;

  private final AtomicBoolean isCrashing = new AtomicBoolean(false);

  @Before
  public void setup() {
    MemberVM locator = clusterStartUp.startLocatorVM(0);
    int locatorPort = locator.getPort();
    MemberVM server1 = clusterStartUp.startRedisVM(1, locatorPort);
    MemberVM server2 = clusterStartUp.startRedisVM(2, locatorPort);
    MemberVM server3 = clusterStartUp.startRedisVM(3, locatorPort);
    servers = new ArrayList<>();
    servers.add(server1);
    servers.add(server2);
    servers.add(server3);

    int redisServerPort1 = clusterStartUp.getRedisPort(1);

    jedis =
        new JedisCluster(new HostAndPort(BIND_ADDRESS, redisServerPort1), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    jedis.close();
  }

  @Test
  public void ZRemRangeByLex_removesMembersInRange() {
    Map<String, Double> memberScoreMap = makeMemberScoreMap(SET_SIZE);
    jedis.zadd(KEY, memberScoreMap);
    verifyDataExists(memberScoreMap);

    List<String> expected = new ArrayList<>(memberScoreMap.keySet().stream().sorted()
        .collect(Collectors.toList())).subList(4, SET_SIZE);

    assertThat(jedis.zrank(KEY, "vvvvv")).isEqualTo(4L);

    assertThat(jedis.zremrangeByLex(KEY, "[" + "v", "[" + "vvvv")).isEqualTo(4);

    assertThat(jedis.zrank(KEY, "vvvvv")).isEqualTo(0L);

    assertThat(jedis.zrange(KEY, 0, -1)).hasSameElementsAs(expected);
  }

  @Test
  public void ZRemRangeByLex_concurrentlyRemovesMembersInRange() {
    Map<String, Double> memberScoreMap = makeMemberScoreMap(SET_SIZE);
    jedis.zadd(KEY, memberScoreMap);
    verifyDataExists(memberScoreMap);

    assertThat(jedis.zrank(KEY, "vvvvv")).isEqualTo(4L);

    AtomicInteger totalRemoved = new AtomicInteger();
    new ConcurrentLoopingThreads(SET_SIZE,
        (i) -> doZRemRangeByLexOnMembers(i, totalRemoved),
        (i) -> doZRemRangeByLexOnMembersInDifferentOrder(i, totalRemoved)).run();

    assertThat(jedis.exists(KEY)).isFalse();

    assertThat(totalRemoved.get()).isEqualTo(SET_SIZE);
  }

  @Test
  public void zRemRangeByLexRemovesMembersFromSortedSetAfterPrimaryShutsDown() {
    Map<String, Double> memberScoreMap = makeMemberScoreMap(SET_SIZE);
    jedis.zadd(KEY, memberScoreMap);
    verifyDataExists(memberScoreMap);

    stopNodeWithPrimaryBucketOfTheKey(false);

    doZRemRangeByLexWithRetries(memberScoreMap);

    verifyDataDoesNotExist(memberScoreMap);
    assertThat(jedis.exists(KEY)).isFalse();
  }

  @Test
  public void zRemRangeByLexCanRemoveMembersFromSortedSetWhenPrimaryIsCrashed() throws Exception {
    Map<String, Double> memberScoreMap = makeMemberScoreMap(SET_SIZE);

    jedis.zadd(KEY, memberScoreMap);
    verifyDataExists(memberScoreMap);

    memberScoreMap.remove(MEMBER_BASE_STRING);

    Future<Void> future1 = executor.submit(() -> stopNodeWithPrimaryBucketOfTheKey(true));
    Future<Void> future2 = executor.submit(this::removeAllButFirstEntry);

    future1.get();
    future2.get();

    await().until(() -> verifyDataDoesNotExist(memberScoreMap));
    assertThat(jedis.zrank(KEY, MEMBER_BASE_STRING)).isZero();
    assertThat(jedis.exists(KEY)).isTrue();
  }

  private void removeAllButFirstEntry() {
    long removed = 0;
    int rangeSize = 5;
    await().until(isCrashing::get);
    for (int i = 2; i < SET_SIZE; i += rangeSize) {
      removed +=
          jedis.zremrangeByLex(KEY, "[" + buildMemberName(i), "(" + buildMemberName(i + rangeSize));
    }
    assertThat(removed).isEqualTo(SET_SIZE - 1);
  }

  private void stopNodeWithPrimaryBucketOfTheKey(boolean isCrash) {
    boolean isPrimary;
    for (MemberVM server : servers) {
      isPrimary = server.invoke(ZRemRangeByLexDUnitTest::isPrimaryForKey);
      if (isPrimary) {
        if (isCrash) {
          isCrashing.set(true);
          server.getVM().bounceForcibly();
        } else {
          server.stop();
        }
        return;
      }
    }
  }

  private static boolean isPrimaryForKey() {
    int bucketId = getBucketId(new RedisKey(KEY.getBytes()));
    return isPrimaryForBucket(bucketId);
  }

  private static int getBucketId(Object key) {
    return PartitionedRegionHelper.getHashKey((PartitionedRegion) getDataRegion(), Operation.GET,
        key, null, null);
  }

  private static Region<RedisKey, RedisData> getDataRegion() {
    return ClusterStartupRule.getCache().getRegion(RegionProvider.DEFAULT_REDIS_REGION_NAME);
  }

  private static boolean isPrimaryForBucket(int bucketId) {
    return ((PartitionedRegion) getDataRegion()).getLocalPrimaryBucketsListTestOnly()
        .contains(bucketId);
  }

  private Map<String, Double> makeMemberScoreMap(int setSize) {
    Map<String, Double> scoreMemberPairs = new HashMap<>();
    String memberName = "";
    for (int i = 0; i < setSize; i++) {
      memberName = memberName.concat(MEMBER_BASE_STRING);
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
      removed = jedis.zremrangeByLex(KEY, "[v", "[w");
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
    long count = jedis.zremrangeByLex(KEY, "[" + memberName, "[" + memberName);
    total.addAndGet((int) count);
  }

  private void doZRemRangeByLexOnMembersInDifferentOrder(int i, AtomicInteger total) {
    String memberName = buildMemberName(SET_SIZE - i);
    long count = jedis.zremrangeByLex(KEY, "[" + memberName, "[" + memberName);
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
      Double score = jedis.zscore(KEY, member);
      assertThat(score).isEqualTo(memberScoreMap.get(member));
    }
  }

  private boolean verifyDataDoesNotExist(Map<String, Double> memberScoreMap) {
    try {
      for (String member : memberScoreMap.keySet()) {
        Double score = jedis.zscore(KEY, member);
        assertThat(score).isNull();
      }
    } catch (JedisClusterMaxAttemptsException e) {
      return false;
    }
    return true;
  }
}
