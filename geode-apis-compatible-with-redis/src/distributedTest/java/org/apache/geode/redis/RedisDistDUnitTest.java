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
package org.apache.geode.redis;

import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.categories.RedisTest;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

@Category({RedisTest.class})
public class RedisDistDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule cluster = new RedisClusterStartupRule();

  @ClassRule
  public static ExecutorServiceRule executorService = new ExecutorServiceRule();

  public static final String KEY = "{key}";
  private static JedisCluster jedis;

  @BeforeClass
  public static void setup() {
    MemberVM locator = cluster.startLocatorVM(0);

    cluster.startRedisVM(1, locator.getPort());
    cluster.startRedisVM(2, locator.getPort());

    int server1Port = cluster.getRedisPort(1);
    jedis = new JedisCluster(new HostAndPort(BIND_ADDRESS, server1Port), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void cleanup() {
    cluster.flushAll();
  }

  @Test
  public void testConcurrentSaddOperations_runWithoutException_orDataLoss()
      throws Exception {
    List<String> set1 = new ArrayList<>();
    List<String> set2 = new ArrayList<>();
    int setSize = populateSetValueArrays(set1, set2);

    final String setName = "keyset";

    Future<Void> future = executorService.submit(() -> concurrentSaddOperations(setName, set1));
    concurrentSaddOperations(setName, set2);
    future.get();

    Set<String> smembers = jedis.smembers(setName);

    assertThat(smembers).hasSize(setSize * 2);
    assertThat(smembers).contains(set1.toArray(new String[] {}));
    assertThat(smembers).contains(set2.toArray(new String[] {}));
  }

  private int populateSetValueArrays(List<String> set1, List<String> set2) {
    int setSize = 5000;
    for (int i = 0; i < setSize; i++) {
      set1.add("SETA-" + i);
      set2.add("SETB-" + i);
    }
    return setSize;
  }

  private void concurrentSaddOperations(String key, Collection<String> strings) {
    for (String member : strings) {
      jedis.sadd(key, member);
    }
  }

  @Test
  public void testConcCreateDestroy() throws Exception {
    final int ops = 1000;
    final String hKey = KEY + "hash";
    final String sKey = KEY + "set";
    final String key = KEY + "string";


    // Expect to run with no exception
    Future<Void> future =
        executorService.submit(() -> concurrentCreateDestroy(ops, hKey, sKey, key));
    concurrentCreateDestroy(ops, hKey, sKey, key);

    future.get();

    assertThat(jedis.keys(KEY + "*")).isEmpty();
  }

  private void concurrentCreateDestroy(int ops, String hKey, String sKey, String key) {
    Random r = new Random();
    for (int i = 0; i < ops; i++) {
      int n = r.nextInt(3);
      switch (n) {
        // hashes
        case 0:
          jedis.hset(hKey, randString(), randString());
          jedis.del(hKey);
          break;
        case 1:
          jedis.sadd(sKey, randString());
          jedis.del(sKey);
          break;
        case 2:
          jedis.set(key, randString());
          jedis.del(key);
          break;
      }
    }
  }

  @Test
  public void testConcurrentDel_iteratingOverEachKey() {
    int iterations = 1000;
    String keyBaseName = "DELBASE";

    for (int i = 0; i < iterations; i++) {
      jedis.set(keyBaseName + i, "value" + i);
    }

    AtomicLong deletedCount = new AtomicLong(0);
    new ConcurrentLoopingThreads(iterations,
        (i) -> deletedCount.addAndGet(jedis.del(keyBaseName + i)),
        (i) -> deletedCount.addAndGet(jedis.del(keyBaseName + i)))
            .run();

    assertThat(deletedCount.get()).isEqualTo(iterations);

    for (int i = 0; i < iterations; i++) {
      assertThat(jedis.get(keyBaseName + i)).isNull();
    }
  }

  @Test
  public void testConcurrentDel_bulk() {
    int iterations = 1000;
    String keyBaseName = "{DEL}BASE";

    String[] keys = new String[iterations];
    for (int i = 0; i < iterations; i++) {
      keys[i] = keyBaseName + i;
      jedis.set(keys[i], "value" + i);
    }

    AtomicLong deletedCount = new AtomicLong();
    new ConcurrentLoopingThreads(2,
        (i) -> deletedCount.addAndGet(jedis.del(keys)),
        (i) -> deletedCount.addAndGet(jedis.del(keys)))
            .run();

    assertThat(deletedCount.get()).isEqualTo(iterations);

    for (int i = 0; i < iterations; i++) {
      assertThat(jedis.get(keys[i])).isNull();
    }
  }

  /**
   * Just make sure there are no unexpected server crashes
   */
  @Test
  public void testConcOps() throws Exception {
    final int ops = 100;
    final String hKey = KEY + "hash";
    final String sKey = KEY + "{set}";

    // Expect to run with no exception
    Future<Void> future = executorService.submit(() -> concurrentOps(ops, hKey, sKey));
    concurrentOps(ops, hKey, sKey);

    future.get();
  }

  private void concurrentOps(int ops, String hKey, String sKey) {
    Random r = new Random();
    for (int i = 0; i < ops; i++) {
      int n = r.nextInt(4);
      if (n == 0) {
        jedis.hset(hKey, randString(), randString());
        jedis.hgetAll(hKey);
        jedis.hvals(hKey);
      } else {
        jedis.sadd(sKey, randString());
        jedis.smembers(sKey);
        jedis.sdiff(sKey, sKey + "afd");
        jedis.sunionstore(sKey + "dst", sKey, sKey + "afds");
        jedis.sinterstore(sKey + "dst", sKey, sKey + "afds");
      }
    }
  }

  private String randString() {
    return Long.toHexString(Double.doubleToLongBits(Math.random()));
  }

}
