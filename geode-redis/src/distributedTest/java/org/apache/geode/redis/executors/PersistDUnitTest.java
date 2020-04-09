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
package org.apache.geode.redis.executors;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.RedisTest;

@Category({RedisTest.class})
public class PersistDUnitTest implements Serializable {

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

  private abstract static class ClientTestBase extends SerializableCallable {
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

  class ConcurrentPersistOperation extends ClientTestBase {

    private final String keyBaseName;
    private AtomicLong persistedCount;
    private Long iterationCount;

    protected ConcurrentPersistOperation(int port, String keyBaseName, Long iterationCount) {
      super(port);
      this.keyBaseName = keyBaseName;
      this.persistedCount = new AtomicLong(0);
      this.iterationCount = iterationCount;
    }

    @Override
    public AtomicLong call() {
      Jedis jedis = new Jedis(LOCALHOST, port, JEDIS_TIMEOUT);

      for (int i = 0; i < this.iterationCount; i++) {
        String key = this.keyBaseName + i;
        this.persistedCount.addAndGet(jedis.persist(key));
      }

      return this.persistedCount;
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testConcurrentPersistOperations() throws InterruptedException {
    Long iterationCount = 5000L;
    Jedis jedis = new Jedis(LOCALHOST, server1Port, JEDIS_TIMEOUT);
    setKeysWithExpiration(jedis, iterationCount, "key");

    AsyncInvocation<AtomicLong> remotePersistInvocationClient1 =
        (AsyncInvocation<AtomicLong>) client1
            .invokeAsync(new ConcurrentPersistOperation(server1Port, "key", iterationCount));
    AtomicLong remotePersistInvocationClient2 =
        (AtomicLong) client2.invoke("remotePersistInvocation2",
            new ConcurrentPersistOperation(server2Port, "key", iterationCount));

    remotePersistInvocationClient1.await();

    // Sum of persisted counts returned from both clients should equal total number of keys set
    // (iteration count)
    assertThat(remotePersistInvocationClient2.get() + remotePersistInvocationClient1.get().get())
        .isEqualTo(iterationCount);
  }

  private void setKeysWithExpiration(Jedis jedis, Long iterationCount, String key) {
    for (int i = 0; i < iterationCount; i++) {
      SetParams setParams = new SetParams();
      setParams.ex(600);

      jedis.set(key + i, "value" + i, setParams);
    }
  }
}
