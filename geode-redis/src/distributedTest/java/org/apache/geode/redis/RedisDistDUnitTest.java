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
import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import redis.clients.jedis.Jedis;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.RedisTest;

@Category({RedisTest.class})
@Ignore("GEODE-7905")
public class RedisDistDUnitTest implements Serializable {

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule(5);

  private static String LOCALHOST = "localhost";

  public static final String TEST_KEY = "key";
  private static VM client1;
  private static VM client2;

  private static int server1Port;
  private static int server2Port;

  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().getValueInMS());

  private abstract static class ClientTestBase extends SerializableRunnable {
    int port;

    protected ClientTestBase(int port) {
      this.port = port;
    }
  }

  @BeforeClass
  public static void setup() {
    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    server1Port = ports[0];
    server2Port = ports[1];

    MemberVM locator = cluster.startLocatorVM(0);

    Properties redisProps = new Properties();
    redisProps.setProperty("redis-bind-address", LOCALHOST);
    redisProps.setProperty("redis-port", Integer.toString(ports[0]));
    redisProps.setProperty("log-level", "warn");
    cluster.startServerVM(1, redisProps, locator.getPort());

    redisProps.setProperty("redis-port", Integer.toString(ports[1]));
    cluster.startServerVM(2, redisProps, locator.getPort());

    client1 = cluster.getVM(3);
    client2 = cluster.getVM(4);
  }

  @Test
  public void testConcListOps() throws Exception {
    final Jedis jedis1 = new Jedis(LOCALHOST, server1Port, JEDIS_TIMEOUT);
    final Jedis jedis2 = new Jedis(LOCALHOST, server2Port, JEDIS_TIMEOUT);
    final int pushes = 20;

    class ConcListOps extends ClientTestBase {
      protected ConcListOps(int port) {
        super(port);
      }

      @Override
      public void run() {
        Jedis jedis = new Jedis(LOCALHOST, port, JEDIS_TIMEOUT);
        Random r = new Random();
        for (int i = 0; i < pushes; i++) {
          if (r.nextBoolean()) {
            jedis.lpush(TEST_KEY, randString());
          } else {
            jedis.rpush(TEST_KEY, randString());
          }
        }
      }
    }

    AsyncInvocation<Void> i = client1.invokeAsync(new ConcListOps(server1Port));
    client2.invoke(new ConcListOps(server2Port));
    i.await();
    long expected = 2 * pushes;
    long result1 = jedis1.llen(TEST_KEY);
    long result2 = jedis2.llen(TEST_KEY);
    assertEquals(expected, result1);
    assertEquals(result1, result2);
  }

  @Test
  public void testConcCreateDestroy() throws Exception {

    IgnoredException.addIgnoredException("RegionDestroyedException");
    IgnoredException.addIgnoredException("IndexInvalidException");
    final int ops = 40;
    final String hKey = TEST_KEY + "hash";
    final String lKey = TEST_KEY + "list";
    final String zKey = TEST_KEY + "zset";
    final String sKey = TEST_KEY + "set";

    class ConcCreateDestroy extends ClientTestBase {
      protected ConcCreateDestroy(int port) {
        super(port);
      }

      @Override
      public void run() throws InterruptedException {
        Jedis jedis = new Jedis(LOCALHOST, port, JEDIS_TIMEOUT);
        Random r = new Random();
        for (int i = 0; i < ops; i++) {
          int n = r.nextInt(4);
          if (n == 0) {
            if (r.nextBoolean()) {
              jedis.hset(hKey, randString(), randString());
            } else {
              jedis.del(hKey);
            }
          } else if (n == 1) {
            if (r.nextBoolean()) {
              jedis.lpush(lKey, randString());
            } else {
              jedis.del(lKey);
            }
          } else if (n == 2) {
            if (r.nextBoolean()) {
              jedis.zadd(zKey, r.nextDouble(), randString());
            } else {
              jedis.del(zKey);
            }
          } else {
            if (r.nextBoolean()) {
              jedis.sadd(sKey, randString());
            } else {
              jedis.del(sKey);
            }
          }
        }
      }
    }

    // Expect to run with no exception
    AsyncInvocation<Void> i = client1.invokeAsync(new ConcCreateDestroy(server1Port));
    client2.invoke(new ConcCreateDestroy(server2Port));
    i.await();
  }

  /**
   * Just make sure there are no unexpected server crashes
   */
  @Test
  public void testConcOps() throws Exception {

    final int ops = 100;
    final String hKey = TEST_KEY + "hash";
    final String lKey = TEST_KEY + "list";
    final String zKey = TEST_KEY + "zset";
    final String sKey = TEST_KEY + "set";

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
          } else if (n == 1) {
            jedis.lpush(lKey, randString());
            jedis.rpush(lKey, randString());
            jedis.ltrim(lKey, 0, 100);
            jedis.lrange(lKey, 0, -1);
          } else if (n == 2) {
            jedis.zadd(zKey, r.nextDouble(), randString());
            jedis.zrangeByLex(zKey, "(a", "[z");
            jedis.zrangeByScoreWithScores(zKey, 0, 1, 0, 100);
            jedis.zremrangeByScore(zKey, r.nextDouble(), r.nextDouble());
          } else {
            jedis.sadd(sKey, randString());
            jedis.smembers(sKey);
            jedis.sdiff(sKey, "afd");
            jedis.sunionstore("dst", sKey, "afds");
          }
        }
      }
    }

    // Expect to run with no exception
    AsyncInvocation<Void> i = client1.invokeAsync(new ConcOps(server1Port));
    client2.invoke(new ConcOps(server2Port));
    i.await();
  }

  /**
   * Just make sure there are no unexpected server crashes
   */
  @Test
  public void testConcurrentSadd() throws Exception {
    List<String> set1 = new ArrayList<>();
    List<String> set2 = new ArrayList<>();
    int setSize = 5000;
    for (int i = 0; i < setSize; i++) {
      set1.add("SETA-" + i);
      set2.add("SETB-" + i);
    }

    final String sKey = TEST_KEY + "set";

    class ConcOps extends ClientTestBase {

      private final Collection<String> strings;

      protected ConcOps(int port, Collection<String> strings) {
        super(port);
        this.strings = strings;
      }

      @Override
      public void run() {
        Jedis jedis = new Jedis(LOCALHOST, port, JEDIS_TIMEOUT);
        for (String member : strings) {
          jedis.sadd(sKey, member);
        }
      }
    }

    // Expect to run with no exception
    AsyncInvocation<Void> i = client1.invokeAsync(new ConcOps(server1Port, set1));
    client2.invoke(new ConcOps(server2Port, set2));
    i.await();

    Jedis jedis = new Jedis(LOCALHOST, server1Port, JEDIS_TIMEOUT);
    Set<String> smembers = jedis.smembers(sKey);
    assertThat(smembers).hasSize(setSize * 2);
    assertThat(smembers).contains(set1.toArray(new String[] {}));
    assertThat(smembers).contains(set2.toArray(new String[] {}));
  }

  // @Test
  // public void testConcurrentHset() throws Exception {
  // HashMap<String, String> hashMapA = new HashMap<>();
  // HashMap<String, String> hashMapB = new HashMap<>();
  // int setSize = 100;
  // for (int i = 0; i < setSize; i++) {
  // hashMapA.put("KEY-A-" + i, "VALUE-A-" + i);
  // hashMapB.put("KEY-B-" + i, "VALUE-B-" + i);
  // }
  //
  // class ConcurrentOperations extends ClientTestBase {
  //
  // private final HashMap<String, String> hashMap;
  //
  // protected ConcurrentOperations(int port, HashMap<String, String> hashMap) {
  // super(port);
  // this.hashMap = hashMap;
  // }
  //
  // @Override
  // public void run() {
  // Jedis jedis = new Jedis(LOCALHOST, port, JEDIS_TIMEOUT);
  // for (String key : hashMap.keySet()) {
  // jedis.hset("theSet", key, hashMap.get(key));
  // }
  // }
  // }
  //
  // // Expect to run with no exception
  // AsyncInvocation<Void> i = client1.invokeAsync(new ConcurrentOperations(server1Port, hashMapA));
  // client2.invoke(new ConcurrentOperations(server2Port, hashMapB));
  // i.await();
  //
  // Jedis jedis = new Jedis(LOCALHOST, server1Port, JEDIS_TIMEOUT);
  // Map<String, String> resultHash = jedis.hgetAll("theSet");
  //// assertThat(resultHash).hasSize(setSize * 2);
  //// assertThat(resultHash).contains(set1.toArray(new String[] {}));
  //// assertThat(resultHash).contains(set2.toArray(new String[] {}));
  // }


  private String randString() {
    return Long.toHexString(Double.doubleToLongBits(Math.random()));
  }

}
