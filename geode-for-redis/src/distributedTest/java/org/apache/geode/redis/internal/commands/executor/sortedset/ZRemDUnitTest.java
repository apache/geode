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

import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.services.RegionProvider;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class ZRemDUnitTest {
  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  @Rule
  public RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(4);

  private JedisCluster jedis;
  private List<MemberVM> servers;
  private static final String sortedSetKey = "key";
  private final String baseName = "member1-";
  private final int setSize = 1000;

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
  }

  @After
  public void tearDown() {
    jedis.close();
  }

  @Test
  public void zRemCanRemoveMembersFromSortedSet() {
    Map<String, Double> memberScoreMap = makeMemberScoreMap(setSize);
    jedis.zadd(sortedSetKey, memberScoreMap);
    verifyDataExists(memberScoreMap);

    long removed = jedis.zrem(sortedSetKey, memberScoreMap.keySet().toArray(new String[] {}));
    assertThat(removed).isEqualTo(setSize);

    verifyDataDoesNotExist(memberScoreMap);
    assertThat(jedis.exists(sortedSetKey)).isFalse();
  }

  @Test
  public void zRemCanRemoveMembersConcurrentlyFromSortedSet() {
    Map<String, Double> memberScoreMap = makeMemberScoreMap(setSize);
    jedis.zadd(sortedSetKey, memberScoreMap);
    verifyDataExists(memberScoreMap);

    AtomicInteger totalRemoved = new AtomicInteger();
    new ConcurrentLoopingThreads(setSize,
        (i) -> doZRemOnMembers(i, totalRemoved),
        (i) -> doZRemOnMembersInDifferentOrder(i, totalRemoved)).run();

    assertThat(totalRemoved.get()).isEqualTo(setSize);
    assertThat(jedis.exists(sortedSetKey)).isFalse();
  }

  private void doZRemOnMembers(int i, AtomicInteger total) {
    long count = jedis.zrem(sortedSetKey, baseName + i);
    total.addAndGet((int) count);
  }

  private void doZRemOnMembersInDifferentOrder(int i, AtomicInteger total) {
    long count = jedis.zrem(sortedSetKey, baseName + (setSize - i - 1));
    total.addAndGet((int) count);
  }

  @Test
  public void zRemRemovesMembersFromSortedSetAfterPrimaryShutsDown() {
    Map<String, Double> memberScoreMap = makeMemberScoreMap(setSize);
    jedis.zadd(sortedSetKey, memberScoreMap);
    verifyDataExists(memberScoreMap);

    stopNodeWithPrimaryBucketOfTheKey(false);

    doZRemWithRetries(memberScoreMap);

    verifyDataDoesNotExist(memberScoreMap);
    assertThat(jedis.exists(sortedSetKey)).isFalse();
  }

  private void doZRemWithRetries(Map<String, Double> map) {
    int maxRetryAttempts = 10;
    int retryAttempts = 0;
    while (!zRemWithRetries(map, retryAttempts, maxRetryAttempts)) {
      retryAttempts++;
    }
  }

  private boolean zRemWithRetries(Map<String, Double> map, int retries, int maxRetries) {
    long removed;
    try {
      removed = jedis.zrem(sortedSetKey, map.keySet().toArray(new String[] {}));
    } catch (JedisClusterMaxAttemptsException e) {
      if (retries < maxRetries) {
        return false;
      }
      throw e;
    }
    assertThat(removed).isEqualTo(map.size());
    return true;
  }

  private void doZRem(Map<String, Double> map) {
    long removed = jedis.zrem(sortedSetKey, map.keySet().toArray(new String[] {}));
    assertThat(removed).isEqualTo(map.size());
  }

  @Ignore("tracked by GEODE-9691")
  @Test
  public void zRemCanRemoveMembersFromSortedSetDuringPrimaryIsCrashed() throws Exception {
    int mapSize = 300;
    Map<String, Double> memberScoreMap = makeMemberScoreMap(mapSize);

    jedis.zadd(sortedSetKey, memberScoreMap);
    verifyDataExists(memberScoreMap);

    int number = 10;
    String memberNotRemoved = baseName + number;
    memberScoreMap.remove(memberNotRemoved);

    Future<Void> future1 = executor.submit(() -> doZRem(memberScoreMap));
    Future<Void> future2 = executor.submit(() -> stopNodeWithPrimaryBucketOfTheKey(true));

    future1.get();
    future2.get();

    GeodeAwaitility.await().until(() -> verifyDataDoesNotExist(memberScoreMap));
    assertThat(jedis.exists(sortedSetKey)).isTrue();
  }

  private void verifyDataExists(Map<String, Double> memberScoreMap) {
    for (String member : memberScoreMap.keySet()) {
      Double score = jedis.zscore(sortedSetKey, member);
      assertThat(score).isEqualTo(memberScoreMap.get(member));
    }
  }

  private boolean verifyDataDoesNotExist(Map<String, Double> memberScoreMap) {
    try {
      for (String member : memberScoreMap.keySet()) {
        Double score = jedis.zscore(sortedSetKey, member);
        assertThat(score).isNull();
      }
    } catch (JedisClusterMaxAttemptsException e) {
      return false;
    }
    return true;
  }

  private void stopNodeWithPrimaryBucketOfTheKey(boolean isCrash) {
    boolean isPrimary;
    for (MemberVM server : servers) {
      isPrimary = server.invoke(ZRemDUnitTest::isPrimaryForKey);
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
    return ClusterStartupRule.getCache().getRegion(RegionProvider.DEFAULT_REDIS_REGION_NAME);
  }

  private static boolean isPrimaryForBucket(int bucketId) {
    return ((PartitionedRegion) getDataRegion()).getLocalPrimaryBucketsListTestOnly()
        .contains(bucketId);
  }

  private Map<String, Double> makeMemberScoreMap(int setSize) {
    Map<String, Double> scoreMemberPairs = new HashMap<>();
    for (int i = 0; i < setSize; i++) {
      scoreMemberPairs.put(baseName + i, Double.valueOf(i + ""));
    }
    return scoreMemberPairs;
  }
}
