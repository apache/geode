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
package org.apache.geode.redis.internal.commands.executor.sortedset;

import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static redis.clients.jedis.BinaryJedisCluster.DEFAULT_MAX_ATTEMPTS;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.After;
import org.junit.Before;
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
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.services.RegionProvider;
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
  private final List<MemberVM> servers = new ArrayList<>();
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
    servers.add(clusterStartUp.startRedisVM(1, locatorPort));
    servers.add(clusterStartUp.startRedisVM(2, locatorPort));
    servers.add(clusterStartUp.startRedisVM(3, locatorPort));

    int redisServerPort = clusterStartUp.getRedisPort(1);

    // making SO_TIMEOUT smaller so that crash and stop tests do not take minutes to run.
    final int SO_TIMEOUT = 10000;
    jedis = new JedisCluster(new HostAndPort(BIND_ADDRESS, redisServerPort), REDIS_CLIENT_TIMEOUT,
        SO_TIMEOUT, DEFAULT_MAX_ATTEMPTS, new GenericObjectPoolConfig<>());
  }

  @After
  public void tearDown() {
    jedis.close();
  }

  @Test
  public void zAddWithIncrOptionCanAddAndIncrementScoresConcurrently() {
    new ConcurrentLoopingThreads(setSize,
        (i) -> doConcurrentZAddIncr(i, increment1),
        (i) -> doConcurrentZAddIncr(i, increment2)).run();

    assertThat(jedis.zcard(sortedSetKey)).isEqualTo(setSize);
    verifyZScores(false);
  }

  private void verifyZScores(boolean withPrimaryCrash) {
    for (int i = 0; i < setSize; i++) {
      if (withPrimaryCrash) {
        assertThat(jedis.zscore(sortedSetKey, baseMemberName + i)).isIn(total, total + increment2);
      } else {
        assertThat(jedis.zscore(sortedSetKey, baseMemberName + i)).isEqualTo(total);
      }
    }
  }

  private void doConcurrentZAddIncr(int i, double increment) {
    assertThat(doZAddIncr(i, increment)).isIn(increment, total);
  }

  private void doCrashZAddIncr(int i) {
    // When the primary crashes jedis will retry the operation.
    // But the operation being done during the crash may have finished on the secondary.
    // In that case the retry results in the increment being done twice.
    assertThat(doZAddIncr(i, increment2)).isIn(total, total + increment2);
  }

  private void doNormalZAddIncr(int i, double increment, double total) {
    assertThat(doZAddIncr(i, increment)).isEqualTo(total);
  }

  private double doZAddIncr(int i, double increment) {
    Object result =
        jedis.sendCommand(sortedSetKey, Protocol.Command.ZADD, sortedSetKey, "INCR",
            String.valueOf(increment), baseMemberName + i);
    return Double.parseDouble(new String((byte[]) result));
  }

  @Test
  public void zAddWithIncrOptionCanIncrementScoresAfterPrimaryShutsDown() {
    doZAddIncrForAllMembers(increment1, increment1);

    stopNodeWithPrimaryBucketOfTheKey(false);

    doZCardWithRetries();
    doZAddIncrForAllMembers(increment2, total);
    verifyZScores(false);
  }

  private void doZAddIncrForAllMembers(double increment1, double increment2) {
    for (int i = 0; i < setSize; i++) {
      doNormalZAddIncr(i, increment1, increment2);
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

  @Test
  public void zAddWithIncrOptionCanIncrementScoresDuringPrimaryIsCrashed() throws Exception {
    AtomicBoolean hitJedisClusterIssue2347 = new AtomicBoolean(false);
    doZAddIncrForAllMembers(increment1, increment1);

    Future<Void> future1 =
        executor.submit(() -> doZAddIncrForAllMembersDuringCrash(hitJedisClusterIssue2347));
    Future<Void> future2 = executor.submit(() -> stopNodeWithPrimaryBucketOfTheKey(true));

    future1.get();
    future2.get();

    if (!hitJedisClusterIssue2347.get()) {
      doZCardWithRetries();
      verifyZScores(true);
    }
  }

  private void doZAddIncrForAllMembersDuringCrash(AtomicBoolean hitJedisClusterIssue2347) {
    for (int i = 0; i < setSize; i++) {
      try {
        doCrashZAddIncr(i);
      } catch (JedisClusterMaxAttemptsException ignore) {
        hitJedisClusterIssue2347.set(true);
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
}
