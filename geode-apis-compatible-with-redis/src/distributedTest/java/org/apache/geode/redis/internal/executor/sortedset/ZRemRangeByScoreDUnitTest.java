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

import static java.util.Objects.requireNonNull;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
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
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class ZRemRangeByScoreDUnitTest {
  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  @Rule
  public RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(4);

  private JedisCluster jedis;
  private List<MemberVM> servers;
  private static final String KEY = "ZRemRangeByScoreDUnitTestKey";
  private static final String BASE_MEMBER_NAME = "member";
  private final int SET_SIZE = 500;
  private final AtomicBoolean isCrashing = new AtomicBoolean(false);

  @Before
  public void setup() {
    MemberVM locator = clusterStartUp.startLocatorVM(0);
    int locatorPort = locator.getPort();
    int[] redisPorts = AvailablePortHelper.getRandomAvailableTCPPorts(3);
    MemberVM server1 = clusterStartUp.startRedisVM(1, redisPorts[0], locatorPort);
    MemberVM server2 = clusterStartUp.startRedisVM(2, redisPorts[1], locatorPort);
    MemberVM server3 = clusterStartUp.startRedisVM(3, redisPorts[2], locatorPort);
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
  public void zRemRangeByScore_removesMembersInRangeFromBothServers() {
    Map<String, Double> memberScoreMap = makeMemberScoreMap();
    jedis.zadd(KEY, memberScoreMap);
    verifyDataExists(memberScoreMap);

    int removeRangeSize = 4;
    List<String> expected = new ArrayList<>(memberScoreMap.keySet().stream().sorted()
        .collect(Collectors.toList())).subList(removeRangeSize, SET_SIZE);

    // Subtract 1 from removeRangeSize here since scores start from 0, not 1
    assertThat(jedis.zremrangeByScore(KEY, "0", String.valueOf(removeRangeSize - 1)))
        .isEqualTo(removeRangeSize);

    for (int i = 0; i < removeRangeSize; ++i) {
      assertThat(jedis.zrank(KEY, BASE_MEMBER_NAME + i)).isNull();
    }

    assertThat(jedis.zrange(KEY, 0, -1)).containsExactlyElementsOf(expected);
  }

  @Test
  public void zRemRangeByScore_concurrentlyRemovesMembersInRangeFromBothServers() {
    Map<String, Double> memberScoreMap = makeMemberScoreMap();
    jedis.zadd(KEY, memberScoreMap);
    verifyDataExists(memberScoreMap);

    AtomicInteger totalRemoved = new AtomicInteger();
    int rangeSize = 5;
    new ConcurrentLoopingThreads(SET_SIZE / rangeSize,
        (i) -> doZRemRangeByScore(i * rangeSize, totalRemoved),
        (i) -> doZRemRangeByScore(i * rangeSize, totalRemoved)).run();

    assertThat(jedis.exists(KEY)).isFalse();

    assertThat(totalRemoved.get()).isEqualTo(SET_SIZE);
  }

  @Test
  public void zRemRangeByScoreRemovesMembersFromSortedSetAfterPrimaryShutsDown() {
    Map<String, Double> memberScoreMap = makeMemberScoreMap();
    jedis.zadd(KEY, memberScoreMap);
    verifyDataExists(memberScoreMap);

    stopNodeWithPrimaryBucketOfTheKey(false);

    doZRemRangeByScoreWithRetries(memberScoreMap);

    verifyDataDoesNotExist(memberScoreMap);
    assertThat(jedis.exists(KEY)).isFalse();
  }

  @Test
  public void zRemRangeByScoreCanRemoveMembersFromSortedSetWhenPrimaryIsCrashed()
      throws ExecutionException, InterruptedException {
    Map<String, Double> memberScoreMap = makeMemberScoreMap();

    jedis.zadd(KEY, memberScoreMap);
    verifyDataExists(memberScoreMap);

    String firstMember = String.join("", jedis.zrange(KEY, 0, 0));
    memberScoreMap.remove(firstMember);

    Future<Void> future1 = executor.submit(() -> stopNodeWithPrimaryBucketOfTheKey(true));
    Future<Void> future2 = executor.submit(this::removeAllButFirstEntry);

    future1.get();
    future2.get();

    await().until(() -> verifyDataDoesNotExist(memberScoreMap));
    assertThat(jedis.zrank(KEY, firstMember)).isZero();
    assertThat(jedis.exists(KEY)).isTrue();
  }

  private void verifyDataExists(Map<String, Double> memberScoreMap) {
    for (String member : memberScoreMap.keySet()) {
      Double score = jedis.zscore(KEY, member);
      assertThat(score).isEqualTo(memberScoreMap.get(member));
    }
  }

  private void doZRemRangeByScore(int i, AtomicInteger total) {
    long count = jedis.zremrangeByScore(KEY, String.valueOf(i), String.valueOf(i + 5));
    total.addAndGet((int) count);
  }

  private void stopNodeWithPrimaryBucketOfTheKey(boolean isCrash) {
    boolean isPrimary;
    for (MemberVM server : servers) {
      isPrimary = server.invoke(ZRemRangeByScoreDUnitTest::isPrimaryForKey);
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

  private void removeAllButFirstEntry() {
    long removed = 0;
    int rangeSize = 10;
    await().until(isCrashing::get);
    for (int i = 1; i < SET_SIZE; i += rangeSize) {
      removed += jedis.zremrangeByScore(KEY, String.valueOf(i), String.valueOf(i + rangeSize));
    }
    assertThat(removed).isEqualTo(SET_SIZE - 1);
  }

  private static boolean isPrimaryForKey() {
    PartitionedRegion region = (PartitionedRegion) requireNonNull(ClusterStartupRule.getCache())
        .getRegion(RegionProvider.REDIS_DATA_REGION);
    int bucketId = PartitionedRegionHelper
        .getHashKey(region, Operation.GET, new RedisKey(KEY.getBytes()), null, null);
    return region.getLocalPrimaryBucketsListTestOnly().contains(bucketId);
  }

  private Map<String, Double> makeMemberScoreMap() {
    Map<String, Double> scoreMemberPairs = new HashMap<>();
    String memberName = BASE_MEMBER_NAME;
    for (int i = 0; i < SET_SIZE; i++) {
      memberName = memberName + i;
      scoreMemberPairs.put(memberName, (double) i);
    }
    return scoreMemberPairs;
  }

  private void doZRemRangeByScoreWithRetries(Map<String, Double> map) {
    int maxRetryAttempts = 10;
    int retryAttempts = 0;
    while (!zRemRangeByScoreWithRetries(map, retryAttempts, maxRetryAttempts)) {
      retryAttempts++;
    }
  }

  private boolean zRemRangeByScoreWithRetries(Map<String, Double> map, int retries,
      int maxRetries) {
    long removed;
    try {
      removed = jedis.zremrangeByScore(KEY, "-inf", "+inf");
    } catch (JedisClusterMaxAttemptsException e) {
      if (retries < maxRetries) {
        return false;
      }
      throw e;
    }
    assertThat(removed).isEqualTo(map.size());
    return true;
  }
}
