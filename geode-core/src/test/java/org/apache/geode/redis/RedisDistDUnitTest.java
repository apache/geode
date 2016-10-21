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

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.util.Random;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import redis.clients.jedis.Jedis;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;

@Category(DistributedTest.class)
public class RedisDistDUnitTest extends JUnit4DistributedTestCase {

  public static final String TEST_KEY = "key";
  public static int pushes = 200;
  int redisPort = 6379;
  private Host host;
  private VM server1;
  private VM server2;
  private VM client1;
  private VM client2;

  private int server1Port;
  private int server2Port;

  private String localHost;

  private static final int JEDIS_TIMEOUT = 20 * 1000;

  private abstract class ClientTestBase extends SerializableCallable {

    int port;

    protected ClientTestBase(int port) {
      this.port = port;
    }
  }

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS();

    localHost = SocketCreator.getLocalHost().getHostName();

    host = Host.getHost(0);
    server1 = host.getVM(0);
    server2 = host.getVM(1);
    client1 = host.getVM(2);
    client2 = host.getVM(3);
    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    final int locatorPort = DistributedTestUtils.getDUnitLocatorPort();
    final SerializableCallable<Object> startRedisAdapter = new SerializableCallable<Object>() {

      @Override
      public Object call() throws Exception {
        int port = ports[VM.getCurrentVMNum()];
        CacheFactory cF = new CacheFactory();
        String locator = SocketCreator.getLocalHost().getHostName() + "[" + locatorPort + "]";
        cF.set(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
        cF.set(ConfigurationProperties.REDIS_BIND_ADDRESS, localHost);
        cF.set(ConfigurationProperties.REDIS_PORT, "" + port);
        cF.set(MCAST_PORT, "0");
        cF.set(LOCATORS, locator);
        cF.create();
        return Integer.valueOf(port);
      }
    };
    AsyncInvocation i = server1.invokeAsync(startRedisAdapter);
    server2Port = (Integer) server2.invoke(startRedisAdapter);
    server1Port = (Integer) i.getResult();
  }

  @Override
  public final void preTearDown() throws Exception {
    disconnectAllFromDS();
  }

  @Category(FlakyTest.class) // GEODE-1092: random ports, failure stack involves TCPTransport
                             // ConnectionHandler (are we eating BindExceptions somewhere?), uses
                             // Random, async actions
  @Test
  public void testConcListOps() throws Exception {
    final Jedis jedis1 = new Jedis(localHost, server1Port, JEDIS_TIMEOUT);
    final Jedis jedis2 = new Jedis(localHost, server2Port, JEDIS_TIMEOUT);
    final int pushes = 20;
    class ConcListOps extends ClientTestBase {
      protected ConcListOps(int port) {
        super(port);
      }

      @Override
      public Object call() throws Exception {
        Jedis jedis = new Jedis(localHost, port, JEDIS_TIMEOUT);
        Random r = new Random();
        for (int i = 0; i < pushes; i++) {
          if (r.nextBoolean()) {
            jedis.lpush(TEST_KEY, randString());
          } else {
            jedis.rpush(TEST_KEY, randString());
          }
        }
        return null;
      }
    };

    AsyncInvocation i = client1.invokeAsync(new ConcListOps(server1Port));
    client2.invoke(new ConcListOps(server2Port));
    i.getResult();
    long expected = 2 * pushes;
    long result1 = jedis1.llen(TEST_KEY);
    long result2 = jedis2.llen(TEST_KEY);
    assertEquals(expected, result1);
    assertEquals(result1, result2);
  }

  @Category(FlakyTest.class) // GEODE-717: random ports, BindException in failure stack, async
                             // actions
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
      public Object call() throws Exception {
        Jedis jedis = new Jedis(localHost, port, JEDIS_TIMEOUT);
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
        return null;
      }
    }

    // Expect to run with no exception
    AsyncInvocation i = client1.invokeAsync(new ConcCreateDestroy(server1Port));
    client2.invoke(new ConcCreateDestroy(server2Port));
    i.getResult();
  }

  /**
   * Just make sure there are no unexpected server crashes
   */
  @Category(FlakyTest.class) // GEODE-1697
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
      public Object call() throws Exception {
        Jedis jedis = new Jedis(localHost, port, JEDIS_TIMEOUT);
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
        return null;
      }
    }

    // Expect to run with no exception
    AsyncInvocation i = client1.invokeAsync(new ConcOps(server1Port));
    client2.invoke(new ConcOps(server2Port));
    i.getResult();
  }

  private String randString() {
    return Long.toHexString(Double.doubleToLongBits(Math.random()));
  }

}
