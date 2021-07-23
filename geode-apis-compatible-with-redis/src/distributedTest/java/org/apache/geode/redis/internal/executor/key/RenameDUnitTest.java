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
package org.apache.geode.redis.internal.executor.key;


import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.exceptions.JedisMovedDataException;

import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.services.StripedCoordinator;
import org.apache.geode.redis.internal.services.SynchronizedStripedCoordinator;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class RenameDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(3);

  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  private static JedisCluster jedisCluster;
  private static MemberVM locator;

  @BeforeClass
  public static void setup() {
    locator = clusterStartUp.startLocatorVM(0);
    clusterStartUp.startRedisVM(1, locator.getPort());
    clusterStartUp.startRedisVM(2, locator.getPort());
    clusterStartUp.startRedisVM(3, locator.getPort());

    int redisServerPort1 = clusterStartUp.getRedisPort(1);
    jedisCluster =
        new JedisCluster(new HostAndPort(BIND_ADDRESS, redisServerPort1), REDIS_CLIENT_TIMEOUT);
  }

  @Before
  public void testSetup() {
    clusterStartUp.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedisCluster.close();
  }

  @Test
  public void testRenameWithKeysOnAnyStripeOrServer()
      throws ExecutionException, InterruptedException {
    int numRenames = 10000;

    List<String> listOfKeys = new ArrayList<>(getKeysOnAnyStripe(numRenames * 8));

    listOfKeys.forEach(key -> jedisCluster.sadd(key, "value"));

    for (int i = 0; i < numRenames; i++) {
      int index = i * 8;
      doConcurrentRenames(listOfKeys.subList(index, index + 2),
          listOfKeys.subList(index + 2, index + 4),
          listOfKeys.subList(index + 4, index + 6),
          listOfKeys.subList(index + 6, index + 8));
    }
  }

  @Test
  public void testRenameWithKeysOnSameStripeDifferentServers()
      throws ExecutionException, InterruptedException {
    int numRenames = 10000;

    List<String> listOfKeys = new ArrayList<>(getKeysOnSameRandomStripe(numRenames * 8));

    listOfKeys.forEach(key -> jedisCluster.sadd(key, "value"));

    for (int i = 0; i < numRenames; i++) {
      int index = i * 8;
      doConcurrentRenames(listOfKeys.subList(index, index + 2),
          listOfKeys.subList(index + 2, index + 4), listOfKeys.subList(index + 4, index + 6),
          listOfKeys.subList(index + 6, index + 8));
    }
  }

  @Test
  public void testRenameWithKeysOnDifferentServers_shouldReturnMovedError() {
    int port1 = clusterStartUp.getRedisPort(1);
    Jedis jedis = new Jedis(BIND_ADDRESS, port1, REDIS_CLIENT_TIMEOUT);

    String srcKey = clusterStartUp.getKeyOnServer("key-", 1);
    String dstKey = clusterStartUp.getKeyOnServer("key-", 2);

    jedis.set(srcKey, "Fancy that");

    assertThatThrownBy(() -> jedis.rename(srcKey, dstKey))
        .isInstanceOf(JedisMovedDataException.class);
  }

  private Set<String> getKeysOnSameRandomStripe(int numKeysNeeded) {
    Random random = new Random();
    String key1 = "{rename}keyz" + random.nextInt();
    RedisKey key1RedisKey = new RedisKey(key1.getBytes());
    StripedCoordinator stripedCoordinator = new SynchronizedStripedCoordinator();
    Set<String> keys = new HashSet<>();
    keys.add(key1);

    do {
      String key2 = "{rename}key" + random.nextInt();
      if (stripedCoordinator.compareStripes(key1RedisKey,
          new RedisKey(key2.getBytes())) == 0) {
        keys.add(key2);
      }
    } while (keys.size() < numKeysNeeded);

    return keys;
  }

  private Set<String> getKeysOnAnyStripe(int numKeysNeeded) {
    Random random = new Random();
    Set<String> keys = new HashSet<>();

    do {
      String key = "{rename}key" + random.nextInt();
      keys.add(key);
    } while (keys.size() < numKeysNeeded);

    return keys;
  }

  private void doConcurrentRenames(List<String> listOfKeys1, List<String> listOfKeys2,
      List<String> listOfKeys3, List<String> listOfKeys4)
      throws ExecutionException, InterruptedException {
    CyclicBarrier startCyclicBarrier = new CyclicBarrier(4);

    String oldKey1 = listOfKeys1.get(0);
    String newKey1 = listOfKeys1.get(1);
    String oldKey2 = listOfKeys2.get(0);
    String newKey2 = listOfKeys2.get(1);
    String oldKey3 = listOfKeys3.get(0);
    String newKey3 = listOfKeys3.get(1);
    String oldKey4 = listOfKeys4.get(0);
    String newKey4 = listOfKeys4.get(1);

    Callable<String> renameOldKey1ToNewKey1 = () -> {
      cyclicBarrierAwait(startCyclicBarrier);
      return jedisCluster.rename(oldKey1, newKey1);
    };

    Callable<String> renameOldKey2ToNewKey2 = () -> {
      cyclicBarrierAwait(startCyclicBarrier);
      return jedisCluster.rename(oldKey2, newKey2);
    };

    Callable<String> renameOldKey3ToNewKey3 = () -> {
      cyclicBarrierAwait(startCyclicBarrier);
      return jedisCluster.rename(oldKey3, newKey3);
    };

    Callable<String> renameOldKey4ToNewKey4 = () -> {
      cyclicBarrierAwait(startCyclicBarrier);
      return jedisCluster.rename(oldKey4, newKey4);
    };

    Future<?> future1 = executor.submit(renameOldKey1ToNewKey1);
    Future<?> future2 = executor.submit(renameOldKey2ToNewKey2);
    Future<?> future3 = executor.submit(renameOldKey3ToNewKey3);
    Future<?> future4 = executor.submit(renameOldKey4ToNewKey4);

    future1.get();
    future2.get();
    future3.get();
    future4.get();
  }

  @Test
  public void givenBucketsMoveDuringRename_thenDataIsNotLost() throws Exception {
    AtomicBoolean running = new AtomicBoolean(true);

    List<String> hashtags = new ArrayList<>();
    hashtags.add(clusterStartUp.getKeyOnServer("rename", 1));
    hashtags.add(clusterStartUp.getKeyOnServer("rename", 2));
    hashtags.add(clusterStartUp.getKeyOnServer("rename", 3));

    Runnable task1 = () -> renamePerformAndVerify(1, 10000, hashtags.get(0), running);
    Runnable task2 = () -> renamePerformAndVerify(2, 10000, hashtags.get(1), running);
    Runnable task3 = () -> renamePerformAndVerify(3, 10000, hashtags.get(2), running);

    Future<Void> future1 = executor.runAsync(task1);
    Future<Void> future2 = executor.runAsync(task2);
    Future<Void> future3 = executor.runAsync(task3);

    for (int i = 0; i < 100 && running.get(); i++) {
      clusterStartUp.moveBucketForKey(hashtags.get(i % hashtags.size()));
      GeodeAwaitility.await().during(Duration.ofMillis(200)).until(() -> true);
    }

    running.set(false);

    future1.get();
    future2.get();
    future3.get();
  }

  private void renamePerformAndVerify(int index, int minimumIterations, String hashtag,
      AtomicBoolean isRunning) {
    String baseKey = "{" + hashtag + "}-key-" + index;
    jedisCluster.set(baseKey + "-0", "value");
    int iterationCount = 0;

    while (iterationCount < minimumIterations || isRunning.get()) {
      String oldKey = baseKey + "-" + iterationCount;
      String newKey = baseKey + "-" + (iterationCount + 1);
      try {
        jedisCluster.rename(oldKey, newKey);
      } catch (Exception ex) {
        isRunning.set(false);
        throw new RuntimeException("Exception performing RENAME " + oldKey + " " + newKey, ex);
      }

      assertThat(jedisCluster.exists(newKey))
          .as("key " + newKey + " should exist").isTrue();
      iterationCount += 1;
    }
  }

  private void cyclicBarrierAwait(CyclicBarrier startCyclicBarrier) {
    try {
      startCyclicBarrier.await();
    } catch (InterruptedException | BrokenBarrierException e) {
      throw new RuntimeException(e);
    }
  }
}
