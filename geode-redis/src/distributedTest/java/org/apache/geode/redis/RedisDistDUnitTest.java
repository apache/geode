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
public class RedisDistDUnitTest implements Serializable {

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule(5);

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
    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    server1Port = ports[0];
    server2Port = ports[1];

    MemberVM locator = cluster.startLocatorVM(0);

    Properties redisProps = new Properties();
    redisProps.setProperty("redis-bind-address", LOCALHOST);
    redisProps.setProperty("redis-port", Integer.toString(ports[0]));
    redisProps.setProperty("redis-enabled", "true");
    redisProps.setProperty("log-level", "warn");
    cluster.startServerVM(1, redisProps, locator.getPort());

    redisProps.setProperty("redis-port", Integer.toString(ports[1]));
    cluster.startServerVM(2, redisProps, locator.getPort());

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

    IgnoredException.addIgnoredException("RegionDestroyedException");
    IgnoredException.addIgnoredException("IndexInvalidException");
    final int ops = 40;
    final String hKey = KEY + "hash";
    final String sKey = KEY + "set";

    class ConcCreateDestroy extends ClientTestBase {
      protected ConcCreateDestroy(int port) {
        super(port);
      }

      @Override
      public void run() {
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
