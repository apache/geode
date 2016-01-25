/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.redis;

import java.util.Random;

import redis.clients.jedis.Jedis;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.VM;

public class RedisDistDUnitTest extends DistributedTestCase {

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
  
  private String localHost = SocketCreator.getLocalHost().getHostName();
  
  private static final int JEDIS_TIMEOUT = 20 * 1000;

  private abstract class ClientTestBase extends SerializableCallable {

    int port;
    protected ClientTestBase (int port) {
      this.port = port;
    }

  }

  public RedisDistDUnitTest() throws Throwable {
    super("RedisDistTest");  
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    disconnectAllFromDS();
    host = Host.getHost(0);
    server1 = host.getVM(0);
    server2 = host.getVM(1);
    client1 = host.getVM(2);
    client2 = host.getVM(3);  
    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    final int locatorPort = getDUnitLocatorPort();
    final SerializableCallable<Object> startRedisAdapter = new SerializableCallable<Object>() {

      private static final long serialVersionUID = 1978017907725504294L;

      @Override
      public Object call() throws Exception {
        int port = ports[VM.getCurrentVMNum()];
        CacheFactory cF = new CacheFactory();
        String locator = SocketCreator.getLocalHost().getHostName() + "[" + locatorPort + "]";
        cF.set("log-level", getDUnitLogLevel());
        cF.set("redis-bind-address", localHost);
        cF.set("redis-port", ""+port);
        cF.set("mcast-port", "0");
        cF.set("locators", locator);
        cF.create();
        return Integer.valueOf(port);
      }
    };
    AsyncInvocation i = server1.invokeAsync(startRedisAdapter);
    server2Port = (Integer) server2.invoke(startRedisAdapter);
    try {
      server1Port = (Integer) i.getResult();
    } catch (Throwable e) {
      throw new Exception(e);
    }
  }

  @Override
  public void tearDown2() throws Exception {
    super.tearDown2();
    disconnectAllFromDS();
  }

  public void testConcListOps() throws Throwable {
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

  public void testConcCreateDestroy() throws Throwable {
    addExpectedException("RegionDestroyedException");
    addExpectedException("IndexInvalidException");
    final int ops = 40;
    final String hKey = TEST_KEY+"hash";
    final String lKey = TEST_KEY+"list";
    final String zKey = TEST_KEY+"zset";
    final String sKey = TEST_KEY+"set";

    class ConcCreateDestroy extends ClientTestBase{
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
   * @throws Throwable 
   */
  public void testConcOps() throws Throwable {

    final int ops = 100;
    final String hKey = TEST_KEY+"hash";
    final String lKey = TEST_KEY+"list";
    final String zKey = TEST_KEY+"zset";
    final String sKey = TEST_KEY+"set";

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
