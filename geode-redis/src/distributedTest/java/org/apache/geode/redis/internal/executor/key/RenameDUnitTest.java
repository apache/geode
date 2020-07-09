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

import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.executor.StripedExecutor;
import org.apache.geode.redis.internal.executor.SynchronizedStripedExecutor;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public class RenameDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(3);

  static final String LOCAL_HOST = "127.0.0.1";
  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  ExecutorService pool = Executors.newCachedThreadPool();

  static Jedis jedis;
  static Jedis jedis2;
  static Jedis jedis3;
  static Jedis jedis4;

  static Properties locatorProperties;

  static MemberVM locator;
  static MemberVM server1;
  static MemberVM server2;

  @BeforeClass
  public static void setup() {
    locatorProperties = new Properties();
    locatorProperties.setProperty(MAX_WAIT_TIME_RECONNECT, "15000");

    locator = clusterStartUp.startLocatorVM(0, locatorProperties);
    server1 = clusterStartUp.startRedisVM(1, locator.getPort());
    server2 = clusterStartUp.startRedisVM(2, locator.getPort());

    int redisServerPort1 = clusterStartUp.getRedisPort(1);
    int redisServerPort2 = clusterStartUp.getRedisPort(2);

    jedis = new Jedis(LOCAL_HOST, redisServerPort1, JEDIS_TIMEOUT);
    jedis2 = new Jedis(LOCAL_HOST, redisServerPort1, JEDIS_TIMEOUT);
    jedis3 = new Jedis(LOCAL_HOST, redisServerPort2, JEDIS_TIMEOUT);
    jedis4 = new Jedis(LOCAL_HOST, redisServerPort2, JEDIS_TIMEOUT);
  }

  @Before
  public void testSetup() {
    jedis.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedis.disconnect();
    jedis2.disconnect();
    jedis3.disconnect();
    jedis4.disconnect();

    server1.stop();
    server2.stop();
  }

  @Test
  public void testRenameWithKeysOnAnyStripeOrServer()
      throws ExecutionException, InterruptedException {
    int numRenames = 10000;

    List<String> listOfKeys = new ArrayList<>(getKeysOnAnyStripe(numRenames * 8));

    listOfKeys.forEach(key -> jedis.sadd(key, "value"));

    for (int i = 0; i < numRenames; i++) {
      int index = i * 8;
      doConcurrentRenames(listOfKeys.subList(index, index + 2),
          listOfKeys.subList(index + 2, index + 4), listOfKeys.subList(index + 4, index + 6),
          listOfKeys.subList(index + 6, index + 8));
    }
  }


  @Test
  public void testRenameWithKeysOnSameStripeDifferentServers()
      throws ExecutionException, InterruptedException {
    int numRenames = 10000;

    List<String> listOfKeys = new ArrayList<>(getKeysOnSameRandomStripe(numRenames * 8));

    listOfKeys.forEach(key -> jedis.sadd(key, "value"));

    for (int i = 0; i < numRenames; i++) {
      int index = i * 8;
      doConcurrentRenames(listOfKeys.subList(index, index + 2),
          listOfKeys.subList(index + 2, index + 4), listOfKeys.subList(index + 4, index + 6),
          listOfKeys.subList(index + 6, index + 8));
    }
  }

  private Set<String> getKeysOnSameRandomStripe(int numKeysNeeded) {
    Random random = new Random();
    String key1 = "keyz" + random.nextInt();
    ByteArrayWrapper key1ByteArrayWrapper = new ByteArrayWrapper(key1.getBytes());
    StripedExecutor stripedExecutor = new SynchronizedStripedExecutor();
    Set<String> keys = new HashSet<>();
    keys.add(key1);

    do {
      String key2 = "key" + random.nextInt();
      if (stripedExecutor.compareStripes(key1ByteArrayWrapper,
          new ByteArrayWrapper(key2.getBytes())) == 0) {
        keys.add(key2);
      }
    } while (keys.size() < numKeysNeeded);

    return keys;
  }

  private Set<String> getKeysOnAnyStripe(int numKeysNeeded) {
    Random random = new Random();
    Set<String> keys = new HashSet<>();

    do {
      String key = "key" + random.nextInt();
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

    Runnable renameOldKey1ToNewKey1 = () -> {
      cyclicBarrierAwait(startCyclicBarrier);
      jedis.rename(oldKey1, newKey1);
    };

    Runnable renameOldKey2ToNewKey2 = () -> {
      cyclicBarrierAwait(startCyclicBarrier);
      jedis2.rename(oldKey2, newKey2);
    };

    Runnable renameOldKey3ToNewKey3 = () -> {
      cyclicBarrierAwait(startCyclicBarrier);
      jedis3.rename(oldKey3, newKey3);
    };

    Runnable renameOldKey4ToNewKey4 = () -> {
      cyclicBarrierAwait(startCyclicBarrier);
      jedis4.rename(oldKey4, newKey4);
    };

    Future<?> future1 = pool.submit(renameOldKey1ToNewKey1);
    Future<?> future2 = pool.submit(renameOldKey2ToNewKey2);
    Future<?> future3 = pool.submit(renameOldKey3ToNewKey3);
    Future<?> future4 = pool.submit(renameOldKey4ToNewKey4);

    future1.get();
    future2.get();
    future3.get();
    future4.get();
  }

  private void cyclicBarrierAwait(CyclicBarrier startCyclicBarrier) {
    try {
      startCyclicBarrier.await();
    } catch (InterruptedException | BrokenBarrierException e) {
      throw new RuntimeException(e);
    }
  }
}
