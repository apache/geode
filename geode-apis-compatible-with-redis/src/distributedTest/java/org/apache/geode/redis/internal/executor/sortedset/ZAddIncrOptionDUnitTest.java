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
 *
 */
package org.apache.geode.redis.internal.executor.sortedset;

import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;
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
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class ZAddIncrOptionDUnitTest {
  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  @Rule
  public RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(4);

  private JedisCluster jedis;
  private List<MemberVM> servers;
  private static final String sortedSetKey = "key";
  private final String baseMemberName = "member";
  private final int setSize = 1000;
  private final double increment1 = 355.681000005;
  private final double increment2 = 9554257.921450001;
  private final double total = increment1 + increment2;

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
  public void zAddWithIncrOptionCanAddAndIncrementScoresConcurrently() {
    new ConcurrentLoopingThreads(setSize,
        (i) -> doZAddIncr(i, increment1, total, true),
        (i) -> doZAddIncr(i, increment2, total, true)).run();

    assertThat(jedis.zcard(sortedSetKey)).isEqualTo(setSize);
    verifyZScores();
  }

  private void verifyZScores() {
    for (int i = 0; i < setSize; i++) {
      assertThat(jedis.zscore(sortedSetKey, baseMemberName + i)).isEqualTo(total);
    }
  }

  private void doZAddIncr(int i, double increment, double total, boolean isConcurrentExecution) {
    Object result =
        jedis.sendCommand(sortedSetKey, Protocol.Command.ZADD, sortedSetKey, "INCR",
            Coder.doubleToString(increment), baseMemberName + i);
    if (isConcurrentExecution) {
      assertThat(Coder.bytesToDouble((byte[]) result)).isIn(increment, total);
    } else {
      assertThat(Coder.bytesToDouble((byte[]) result)).isEqualTo(total);
    }
  }

  @Test
  public void zAddWithIncrOptionCanIncrementScoresAfterPrimaryShutsDown() {
    doZAddIncrForAllMembers(increment1, increment1);

    stopNodeWithPrimaryBucketOfTheKey(false);

    doZCardWithRetries();
    doZAddIncrForAllMembers(increment2, total);
    verifyZScores();
  }

  private void doZAddIncrForAllMembers(double increment1, double increment12) {
    for (int i = 0; i < setSize; i++) {
      doZAddIncr(i, increment1, increment12, false);
    }
  }

  private void doZCardWithRetries() {
    int maxRetryAttempts = 10;
    int retryAttempts = 0;
    while (!zCardWithRetries(retryAttempts, maxRetryAttempts)) {
      retryAttempts++;
    }
  }

  private boolean zCardWithRetries(int retries, int maxRetries) {
    long memberSize;
    try {
      memberSize = jedis.zcard(sortedSetKey);
    } catch (JedisClusterMaxAttemptsException e) {
      if (retries < maxRetries) {
        return false;
      }
      throw e;
    }
    assertThat(memberSize).isEqualTo(setSize);
    return true;
  }

  private boolean hitJedisClusterIssue2347 = false;

  @Test
  @Ignore("Fails due to GEODE-9310 and/or GEODE-9311")
  public void zAddWithIncrOptionCanIncrementScoresDuringPrimaryIsCrashed() throws Exception {
    doZAddIncrForAllMembers(increment1, increment1);

    Future<Void> future1 = executor.submit(this::doZAddIncrForAllMembersDuringCrash);
    Future<Void> future2 = executor.submit(() -> stopNodeWithPrimaryBucketOfTheKey(true));

    future1.get();
    future2.get();

    if (!hitJedisClusterIssue2347) {
      doZCardWithRetries();
      verifyZScores();
    }
  }

  private void doZAddIncrForAllMembersDuringCrash() {
    for (int i = 0; i < setSize; i++) {
      try {
        doZAddIncr(i, increment2, total, false);
      } catch (JedisClusterMaxAttemptsException ignore) {
        hitJedisClusterIssue2347 = true;
      }
    }
  }

  private void stopNodeWithPrimaryBucketOfTheKey(boolean isCrash) {
    boolean isPrimary;
    for (MemberVM server : servers) {
      isPrimary = server.invoke(ZAddIncrOptionDUnitTest::isPrimaryForKey);
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
    int bucketId = getBucketId(new RedisKey(Coder.stringToBytes(sortedSetKey)));
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
}
