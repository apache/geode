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

import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.GeodeRedisServerRule;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public class DelIntegrationTest {

  public static Jedis jedis;
  public static Jedis jedis2;
  private static int ITERATION_COUNT = 4000;
  public static int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @ClassRule
  public static GeodeRedisServerRule server = new GeodeRedisServerRule();

  @BeforeClass
  public static void setUp() {
    jedis = new Jedis("localhost", server.getPort(), REDIS_CLIENT_TIMEOUT);
    jedis2 = new Jedis("localhost", server.getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void flushAll() {
    jedis.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();
    jedis2.close();
  }

  @Test
  public void testDel_deletingOneKey_removesKeyAndReturnsOne() {
    String key1 = "firstKey";
    jedis.set(key1, randString());

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
    String key1 = "firstKey";
    String key2 = "secondKey";
    String key3 = "thirdKey";

    jedis.set(key1, randString());
    jedis.set(key2, randString());

    assertThat(jedis.del(key1, key2, key3)).isEqualTo(2L);
    assertThat(jedis.get(key1)).isNull();
    assertThat(jedis.get(key2)).isNull();
  }

  @Test
  public void testConcurrentDel_differentClients() {
    String keyBaseName = "DELBASE";

    new ConcurrentLoopingThreads(
        ITERATION_COUNT,
        (i) -> jedis.set(keyBaseName + i, "value" + i))
            .run();

    AtomicLong deletedCount = new AtomicLong();
    new ConcurrentLoopingThreads(ITERATION_COUNT,
        (i) -> deletedCount.addAndGet(jedis.del(keyBaseName + i)),
        (i) -> deletedCount.addAndGet(jedis2.del(keyBaseName + i)))
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

  private String randString() {
    return Long.toHexString(Double.doubleToLongBits(Math.random()));
  }
}
