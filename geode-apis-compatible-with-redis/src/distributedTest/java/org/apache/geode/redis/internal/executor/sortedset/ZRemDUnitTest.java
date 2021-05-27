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

import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.DEFAULT_MAX_WAIT_TIME_RECONNECT;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class ZRemDUnitTest implements Serializable {
  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  @Rule
  public RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(4);

  private transient JedisCluster jedis;
  private List<MemberVM> servers;
  private final String sortedSetKey = "key";
  private final String baseName = "member1-";
  private final int setSize = 1000;

  private static final String LOCAL_HOST = "127.0.0.1";
  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @Before
  public void setup() {
    Properties locatorProperties = new Properties();
    locatorProperties.setProperty(MAX_WAIT_TIME_RECONNECT, DEFAULT_MAX_WAIT_TIME_RECONNECT);

    MemberVM locator = clusterStartUp.startLocatorVM(0, locatorProperties);
    MemberVM server1 = clusterStartUp.startRedisVM(1, locator.getPort());
    MemberVM server2 = clusterStartUp.startRedisVM(2, locator.getPort());
    MemberVM server3 = clusterStartUp.startRedisVM(3, locator.getPort());
    servers = new ArrayList<>();
    servers.add(server1);
    servers.add(server2);
    servers.add(server3);

    int redisServerPort = clusterStartUp.getRedisPort(1);

    jedis = new JedisCluster(new HostAndPort(LOCAL_HOST, redisServerPort), JEDIS_TIMEOUT);
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
    new ConcurrentLoopingThreads(2,
        (i) -> doZRemOnAllKeysInMap(memberScoreMap, totalRemoved),
        (i) -> doZRemOnAllMembers(totalRemoved)).run();

    assertThat(totalRemoved.get()).isEqualTo(setSize);
    assertThat(jedis.exists(sortedSetKey)).isFalse();
  }

  private void doZRemOnAllKeysInMap(Map<String, Double> map, AtomicInteger total) {
    Set<String> keys = map.keySet();
    for (String key : keys) {
      long count = jedis.zrem(sortedSetKey, key);
      total.addAndGet((int) count);
    }
  }

  private void doZRemOnAllMembers(AtomicInteger total) {
    for (int i = 0; i < setSize; i++) {
      long count = jedis.zrem(sortedSetKey, baseName + i);
      total.addAndGet((int) count);
    }
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

  @Test
  @Ignore("Fails due to GEODE-9310")
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
      isPrimary = server.invoke(this::isPrimaryForKey);
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

  private boolean isPrimaryForKey() {
    int bucketId = getBucketId(new RedisKey(Coder.stringToBytes(sortedSetKey)));
    return isPrimaryForBucket(bucketId);
  }

  private int getBucketId(Object key) {
    return PartitionedRegionHelper.getHashKey((PartitionedRegion) getDataRegion(), Operation.GET,
        key, null, null);
  }

  private Region<RedisKey, RedisData> getDataRegion() {
    return ClusterStartupRule.getCache().getRegion(RegionProvider.REDIS_DATA_REGION);
  }

  private boolean isPrimaryForBucket(int bucketId) {
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
