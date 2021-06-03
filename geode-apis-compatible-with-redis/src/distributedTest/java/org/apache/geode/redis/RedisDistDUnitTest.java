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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import redis.clients.jedis.Jedis;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.categories.RedisTest;

@Category({RedisTest.class})
public class RedisDistDUnitTest implements Serializable {

  @ClassRule
  public static RedisClusterStartupRule cluster = new RedisClusterStartupRule(5);

  private static String LOCALHOST = "localhost";

  public static final String KEY = "key";
  private static VM client1;
  private static VM client2;

  private static int server1Port;
  private static int server2Port;

  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  private abstract static class ClientTestBase extends SerializableRunnable {
    int port;

    protected ClientTestBase(int port) {
      this.port = port;
    }
  }

  @BeforeClass
  public static void setup() {
    MemberVM locator = cluster.startLocatorVM(0);

    cluster.startRedisVM(1, locator.getPort());
    cluster.startRedisVM(2, locator.getPort());

    server1Port = cluster.getRedisPort(1);
    server2Port = cluster.getRedisPort(2);

    client1 = cluster.getVM(3);
    client2 = cluster.getVM(4);
  }

  class ConcurrentSADDOperation extends ClientTestBase {

    private final Collection<String> strings;
    private final String key;

    protected ConcurrentSADDOperation(int port, String key, Collection<String> strings) {
      super(port);
      this.strings = strings;
      this.key = key;
    }

    @Override
    public void run() {
      Jedis jedis = new Jedis(LOCALHOST, port, JEDIS_TIMEOUT);
      for (String member : strings) {
        jedis.sadd(key, member);
      }
    }
  }

  @After
  public void cleanup() {
    cluster.flushAll();
  }

  @Test
  public void testConcurrentSaddOperations_runWithoutException_orDataLoss()
      throws InterruptedException {
    List<String> set1 = new ArrayList<>();
    List<String> set2 = new ArrayList<>();
    int setSize = populateSetValueArrays(set1, set2);

    final String setName = "keyset";

    Jedis jedis = new Jedis(LOCALHOST, server1Port, JEDIS_TIMEOUT);

    AsyncInvocation<Void> remoteSaddInvocation =
        client1.invokeAsync(new ConcurrentSADDOperation(server1Port, setName, set1));

    client2.invoke(new ConcurrentSADDOperation(server2Port, setName, set2));

    remoteSaddInvocation.await();

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

  @Test
  public void testConcCreateDestroy() throws Exception {

    final int ops = 1000;
    final String hKey = KEY + "hash";
    final String sKey = KEY + "set";
    final String key = KEY + "string";

    class ConcCreateDestroy extends ClientTestBase {
      protected ConcCreateDestroy(int port) {
        super(port);
      }

      @Override
      public void run() {
        Jedis jedis = new Jedis(LOCALHOST, port, JEDIS_TIMEOUT);
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
    }

    // Expect to run with no exception
    AsyncInvocation<Void> i = client1.invokeAsync(new ConcCreateDestroy(server1Port));
    client2.invoke(new ConcCreateDestroy(server2Port));
    i.await();

    Jedis jedis = new Jedis(LOCALHOST, server1Port, JEDIS_TIMEOUT);
    assertThat(jedis.keys("*")).isEmpty();
  }

  @Test
  public void testConcurrentDel_iteratingOverEachKey() {
    int iterations = 1000;
    String keyBaseName = "DELBASE";
    Jedis jedis = new Jedis(LOCALHOST, server1Port, JEDIS_TIMEOUT);
    Jedis jedis2 = new Jedis(LOCALHOST, server2Port, JEDIS_TIMEOUT);

    new ConcurrentLoopingThreads(
        iterations,
        (i) -> jedis.set(keyBaseName + i, "value" + i))
            .run();

    AtomicLong deletedCount = new AtomicLong();
    new ConcurrentLoopingThreads(iterations,
        (i) -> deletedCount.addAndGet(jedis.del(keyBaseName + i)),
        (i) -> deletedCount.addAndGet(jedis2.del(keyBaseName + i)))
            .run();

    assertThat(deletedCount.get()).isEqualTo(iterations);

    for (int i = 0; i < iterations; i++) {
      assertThat(jedis.get(keyBaseName + i)).isNull();
    }
  }

  @Test
  public void testConcurrentDel_bulk() {
    int iterations = 1000;
    String keyBaseName = "DELBASE";
    Jedis jedis = new Jedis(LOCALHOST, server1Port, JEDIS_TIMEOUT);
    Jedis jedis2 = new Jedis(LOCALHOST, server2Port, JEDIS_TIMEOUT);

    new ConcurrentLoopingThreads(
        iterations,
        (i) -> jedis.set(keyBaseName + i, "value" + i))
            .run();

    String[] keys = new String[iterations];
    for (int i = 0; i < iterations; i++) {
      keys[i] = keyBaseName + i;
    }

    AtomicLong deletedCount = new AtomicLong();
    new ConcurrentLoopingThreads(2,
        (i) -> deletedCount.addAndGet(jedis.del(keys)),
        (i) -> deletedCount.addAndGet(jedis2.del(keys)))
            .run();

    assertThat(deletedCount.get()).isEqualTo(iterations);

    for (int i = 0; i < iterations; i++) {
      assertThat(jedis.get(keyBaseName + i)).isNull();
    }
  }

  /**
   * Just make sure there are no unexpected server crashes
   */
  @Test
  public void testConcOps() throws Exception {

    final int ops = 100;
    final String hKey = KEY + "hash";
    final String sKey = KEY + "set";

    class ConcOps extends ClientTestBase {

      protected ConcOps(int port) {
        super(port);
      }

      @Override
      public void run() {
        Jedis jedis = new Jedis(LOCALHOST, port, JEDIS_TIMEOUT);
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
            jedis.sdiff(sKey, "afd");
            jedis.sunionstore("dst", sKey, "afds");
            jedis.sinterstore("dst", sKey, "afds");
          }
        }
      }
    }

    // Expect to run with no exception
    AsyncInvocation<Void> i = client1.invokeAsync(new ConcOps(server1Port));
    client2.invoke(new ConcOps(server2Port));
    i.await();
  }

  private String randString() {
    return Long.toHexString(Double.doubleToLongBits(Math.random()));
  }

}
