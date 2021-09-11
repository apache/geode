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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.exceptions.JedisClusterMaxAttemptsException;

import org.apache.geode.cache.Operation;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class ZRemRangeByRankDUnitTest {
  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  @Rule
  public RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(4);

  private JedisCluster jedis;
  private List<MemberVM> servers;
  private static final String KEY = "ZRemRangeByRankDUnitTestKey";
  private static final String BASE_MEMBER_NAME = "member";
  private static final int SET_SIZE = 1000;
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

    int redisServerPort = clusterStartUp.getRedisPort(1);

    jedis = new JedisCluster(new HostAndPort(BIND_ADDRESS, redisServerPort), REDIS_CLIENT_TIMEOUT);
    isCrashing.set(false);
  }

  @After
  public void tearDown() {
    jedis.close();
  }

  @Test
  public void zRemRangeByRankCanRemoveMembersFromSortedSet() {
    Map<String, Double> memberScoreMap = makeMemberScoreMap();
    jedis.zadd(KEY, memberScoreMap);
    verifyDataExists(memberScoreMap);

    long removed = jedis.zremrangeByRank(KEY, 0, -1);
    assertThat(removed).isEqualTo(SET_SIZE);

    verifyDataDoesNotExist(memberScoreMap);
    assertThat(jedis.exists(KEY)).isFalse();
  }

  @Test
  public void zRemRangeByRankCanRemoveMembersConcurrentlyFromSortedSet() {
    Map<String, Double> memberScoreMap = makeMemberScoreMap();
    jedis.zadd(KEY, memberScoreMap);
    verifyDataExists(memberScoreMap);

    AtomicInteger totalRemoved = new AtomicInteger();
    int numberOfMembersToRemove = 10;
    new ConcurrentLoopingThreads(SET_SIZE / numberOfMembersToRemove,
        (i) -> doZRemRangeByRankOnMembers(numberOfMembersToRemove, totalRemoved),
        (i) -> doZRemRangeByRankOnMembersWithOffset(numberOfMembersToRemove,
            numberOfMembersToRemove / 2, totalRemoved)).run();

    assertThat(totalRemoved.get()).isEqualTo(SET_SIZE);
    assertThat(jedis.exists(KEY)).isFalse();
  }

  @Test
  public void zRemRangeByRankRemovesMembersFromSortedSetAfterPrimaryShutsDown() {
    Map<String, Double> memberScoreMap = makeMemberScoreMap();
    jedis.zadd(KEY, memberScoreMap);
    verifyDataExists(memberScoreMap);

    stopNodeWithPrimaryBucketOfTheKey(false);

    doZRemRangeByRankWithRetries(memberScoreMap);

    verifyDataDoesNotExist(memberScoreMap);
    assertThat(jedis.exists(KEY)).isFalse();
  }

  @Test
  public void zRemRangeByRankCanRemoveMembersFromSortedSetWhenPrimaryIsCrashed()
      throws Exception {
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

  private void doZRemRangeByRankOnMembers(int numberOfMemberToRemove, AtomicInteger total) {
    long count = jedis.zremrangeByRank(KEY, 0, numberOfMemberToRemove - 1);
    total.addAndGet((int) count);
  }

  private void doZRemRangeByRankOnMembersWithOffset(int numberOfMemberToRemove, int offset,
      AtomicInteger total) {
    long count = jedis.zremrangeByRank(KEY, offset, (offset + numberOfMemberToRemove) - 1);
    total.addAndGet((int) count);
  }

  private void doZRemRangeByRankWithRetries(Map<String, Double> map) {
    int maxRetryAttempts = 10;
    int retryAttempts = 0;
    while (!zRemRangeByRankWithRetries(map, retryAttempts, maxRetryAttempts)) {
      retryAttempts++;
    }
  }

  private boolean zRemRangeByRankWithRetries(Map<String, Double> map, int retries, int maxRetries) {
    long removed;
    try {
      removed = jedis.zremrangeByRank(KEY, 0, -1);
    } catch (JedisClusterMaxAttemptsException e) {
      if (retries < maxRetries) {
        return false;
      }
      throw e;
    }
    assertThat(removed).isEqualTo(map.size());
    return true;
  }

  private void removeAllButFirstEntry() {
    long removed = 0;
    int rangeSize = 10;
    await().until(isCrashing::get);
    for (int i = 1; i < SET_SIZE; i += rangeSize) {
      removed += jedis.zremrangeByRank(KEY, 1, rangeSize);
    }
    assertThat(removed).isEqualTo(SET_SIZE - 1);
  }

  private void stopNodeWithPrimaryBucketOfTheKey(boolean isCrash) {
    boolean isPrimary;
    for (MemberVM server : servers) {
      isPrimary = server.invoke(ZRemRangeByRankDUnitTest::isPrimaryForKey);
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
    PartitionedRegion region = (PartitionedRegion) ClusterStartupRule.getCache()
        .getRegion(RegionProvider.REDIS_DATA_REGION);
    int bucketId = PartitionedRegionHelper.getHashKey(region, Operation.GET,
        new RedisKey(KEY.getBytes()), null, null);
    return region.getLocalPrimaryBucketsListTestOnly().contains(bucketId);
  }

  private Map<String, Double> makeMemberScoreMap() {
    Map<String, Double> scoreMemberPairs = new HashMap<>();
    for (int i = 0; i < SET_SIZE; i++) {
      scoreMemberPairs.put(BASE_MEMBER_NAME + i, (double) i);
    }
    return scoreMemberPairs;
  }
}
