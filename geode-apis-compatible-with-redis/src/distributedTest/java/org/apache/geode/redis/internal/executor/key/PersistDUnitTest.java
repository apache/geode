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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.categories.RedisTest;

@Category({RedisTest.class})
public class PersistDUnitTest implements Serializable {
  private static final long serialVersionUID = 7647512217349732936L;

  @ClassRule
  public static RedisClusterStartupRule redisClusterStartupRule = new RedisClusterStartupRule(5);
  private static JedisCluster jedis;
  private static final String LOCALHOST = "localhost";

  private static VM client1;
  private static VM client2;
  private static int serverPort;

  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  private abstract static class ClientTestBase extends SerializableCallable<AtomicLong> {
    private static final long serialVersionUID = 7223954380448301617L;

    final int port;

    protected ClientTestBase(int port) {
      this.port = port;
    }
  }

  @BeforeClass
  public static void setup() {
    MemberVM locator = redisClusterStartupRule.startLocatorVM(0);
    redisClusterStartupRule.startRedisVM(1, locator.getPort());
    redisClusterStartupRule.startRedisVM(2, locator.getPort());
    serverPort = redisClusterStartupRule.getRedisPort(1);

    client1 = redisClusterStartupRule.getVM(3);
    client2 = redisClusterStartupRule.getVM(4);

    jedis = new JedisCluster(new HostAndPort(LOCALHOST, serverPort), JEDIS_TIMEOUT);
  }

  static class ConcurrentPersistOperation extends ClientTestBase {
    private static final long serialVersionUID = -4917363339097692860L;

    private final String keyBaseName;
    private final AtomicLong persistedCount;
    private final Long iterationCount;

    protected ConcurrentPersistOperation(int port, String keyBaseName, Long iterationCount) {
      super(port);
      this.keyBaseName = keyBaseName;
      this.persistedCount = new AtomicLong(0);
      this.iterationCount = iterationCount;
    }

    @Override
    public AtomicLong call() {
      JedisCluster internalJedisCluster =
          new JedisCluster(new HostAndPort(LOCALHOST, port), JEDIS_TIMEOUT);

      for (int i = 0; i < this.iterationCount; i++) {
        String key = this.keyBaseName + i;
        this.persistedCount.addAndGet(internalJedisCluster.persist(key));
      }
      return this.persistedCount;
    }
  }

  @Test
  public void testConcurrentPersistOperations_shouldReportCorrectNumberOfTotalKeysPersisted()
      throws InterruptedException {
    Long iterationCount = 5000L;

    setKeysWithExpiration(jedis, iterationCount);

    AsyncInvocation<AtomicLong> remotePersistInvocationClient1 = client1
        .invokeAsync(new ConcurrentPersistOperation(serverPort, "key", iterationCount));

    AtomicLong remotePersistInvocationClient2 = client2.invoke("remotePersistInvocation2",
        new ConcurrentPersistOperation(serverPort, "key", iterationCount));

    remotePersistInvocationClient1.await();

    // Sum of persisted counts returned from both clients should equal total number of keys set
    // (iteration count)
    assertThat(remotePersistInvocationClient2.get() + remotePersistInvocationClient1.get().get())
        .isEqualTo(iterationCount);
  }

  private void setKeysWithExpiration(JedisCluster jedis, Long iterationCount) {
    for (int i = 0; i < iterationCount; i++) {
      jedis.sadd("key" + i, "value" + 9);
      jedis.expire("key" + i, 600);
    }
  }
}
