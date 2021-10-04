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

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertAtLeastNArgs;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractDelIntegrationTest implements RedisIntegrationTest {

  private JedisCluster jedis;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @Before
  public void setUp() {
    jedis = new JedisCluster(new HostAndPort("localhost", getPort()), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    flushAll();
    jedis.close();
  }

  @Test
  public void errors_givenTooFewArguments() {
    assertAtLeastNArgs(jedis, Protocol.Command.DEL, 1);
  }

  @Test
  public void testDel_deletingOneKey_removesKeyAndReturnsOne() {
    String key1 = "firstKey";
    jedis.set(key1, "value1");

    Long deletedCount = jedis.del(key1);

    assertThat(deletedCount).isEqualTo(1L);
    assertThat(jedis.get(key1)).isNull();
  }

  @Test
  public void testDel_deletingNonexistentKey_returnsZero() {
    assertThat(jedis.del("ceci nest pas un clavier")).isEqualTo(0L);
  }

  @Test
  public void testDel_deletingMultipleKeys_returnsCountOfOnlyDeletedKeys() {
    String key1 = "{user1}firstKey";
    String key2 = "{user1}secondKey";
    String key3 = "{user1}thirdKey";

    jedis.set(key1, "value1");
    jedis.set(key2, "value2");

    assertThat(jedis.del(key1, key2, key3)).isEqualTo(2L);
    assertThat(jedis.get(key1)).isNull();
    assertThat(jedis.get(key2)).isNull();
  }

  @Test
  public void testConcurrentDel_differentClients() {
    String keyBaseName = "DELBASE";

    int ITERATION_COUNT = 4000;
    new ConcurrentLoopingThreads(ITERATION_COUNT,
        (i) -> jedis.set(keyBaseName + i, "value" + i))
            .run();

    AtomicLong deletedCount = new AtomicLong();
    new ConcurrentLoopingThreads(ITERATION_COUNT,
        (i) -> deletedCount.addAndGet(jedis.del(keyBaseName + i)),
        (i) -> deletedCount.addAndGet(jedis.del(keyBaseName + i)))
            .run();

    assertThat(deletedCount.get()).isEqualTo(ITERATION_COUNT);

    for (int i = 0; i < ITERATION_COUNT; i++) {
      assertThat(jedis.get(keyBaseName + i)).isNull();
    }
  }

  @Test
  public void testDel_withBinaryKey() {
    byte[] key = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

    jedis.set(key, "foo".getBytes());
    jedis.del(key);

    assertThat(jedis.get(key)).isNull();
  }

}
