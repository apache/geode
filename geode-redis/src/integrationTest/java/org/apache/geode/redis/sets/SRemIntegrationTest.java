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
package org.apache.geode.redis.sets;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.GeodeRedisServer;
import org.apache.geode.test.junit.categories.RedisTest;

@Category({RedisTest.class})
public class SRemIntegrationTest {
  static Jedis jedis;
  static Jedis jedis2;
  private static GeodeRedisServer server;
  private static GemFireCache cache;
  private static int port = 6379;

  @BeforeClass
  public static void setUp() {
    CacheFactory cf = new CacheFactory();
    cf.set(LOG_LEVEL, "error");
    cf.set(MCAST_PORT, "0");
    cf.set(LOCATORS, "");
    cache = cf.create();
    port = AvailablePortHelper.getRandomAvailableTCPPort();
    server = new GeodeRedisServer("localhost", port);

    server.start();
    jedis = new Jedis("localhost", port, 10000000);
    jedis2 = new Jedis("localhost", port, 10000000);
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();
    jedis2.close();
    cache.close();
    server.shutdown();
  }

  @After
  public void cleanup() {
    jedis.flushAll();
  }

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void testSRem_should_ReturnTheNumberOfElementsRemoved() {
    String key = "key";
    String field1 = "field1";
    String field2 = "field2";
    jedis.sadd(key, field1, field2);

    Long removedElementsCount = jedis.srem(key, field1, field2);

    assertThat(removedElementsCount).isEqualTo(2);
  }

  @Test
  public void testSRem_should_RemoveSpecifiedElementsFromSet() {
    String key = "key";
    String field1 = "field1";
    String field2 = "field2";
    jedis.sadd(key, field1, field2);

    jedis.srem(key, field2);

    Set<String> membersInSet = jedis.smembers(key);
    assertThat(membersInSet).doesNotContain(field2);
  }

  @Test
  public void testSRem_should_ThrowError_givenOnlyKey() {
    String key = "key";
    String field1 = "field1";
    String field2 = "field2";
    jedis.sadd(key, field1, field2);

    Throwable caughtException = catchThrowable(
        () -> jedis.srem(key));

    assertThat(caughtException).hasMessageContaining("wrong number of arguments");
  }

  @Test
  public void testSRem_should_notRemoveMembersOfSetNotSpecified() {
    String key = "key";
    String field1 = "field1";
    String field2 = "field2";
    jedis.sadd(key, field1, field2);

    jedis.srem(key, field1);

    Set<String> membersInSet = jedis.smembers(key);
    assertThat(membersInSet).containsExactly(field2);
  }

  @Test
  public void testSRem_should_ignoreMembersNotInSpecifiedSet() {
    String key = "key";
    String field1 = "field1";
    String field2 = "field2";
    String unkownField = "random-guy";
    jedis.sadd(key, field1, field2);

    long result = jedis.srem(key, unkownField);

    assertThat(result).isEqualTo(0);
  }

  @Test
  public void testSRem_should_throwError_givenKeyThatIsNotASet() {
    String key = "key";
    String value = "value";
    jedis.set(key, value);

    Throwable caughtException = catchThrowable(
        () -> jedis.srem(key, value));

    assertThat(caughtException)
        .hasMessageContaining(
            "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  @Test
  public void testSRem_shouldReturnZero_givenNonExistingKey() {
    String key = "key";
    String field = "field";

    long result = jedis.srem(key, field);

    assertThat(result).isEqualTo(0);
  }

  @Test
  public void testSRemErrorMessage_givenIncorrectDataType() {
    jedis.set("farm", "chicken");
    assertThatThrownBy(() -> {
      jedis.srem("farm", "chicken");
    }).isInstanceOf(JedisDataException.class)
        .hasMessageContaining("WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  @Test
  public void testSRemDeletesKeyWhenSetIsEmpty() {
    jedis.sadd("farm", "chicken");

    jedis.srem("farm", "chicken");

    assertThat(jedis.exists("farm")).isFalse();
  }

  @Ignore("GEODE-7905")
  @Test
  public void testConcurrentSRemConsistentlyUpdatesMetaInformation()
      throws ExecutionException, InterruptedException {
    ByteArrayWrapper keyAsByteArray = new ByteArrayWrapper("set".getBytes());
    AtomicLong errorCount = new AtomicLong();
    CyclicBarrier startCyclicBarrier = new CyclicBarrier(2, () -> {
      boolean keyIsRegistered = server.getKeyRegistrar().isRegistered(keyAsByteArray);
      boolean containsKey = server.getRegionCache().getSetRegion().containsKey(keyAsByteArray);

      if (keyIsRegistered != containsKey) {
        errorCount.getAndIncrement();
        jedis.sadd("set", "member");
        jedis.del("set");
      }
    });

    ExecutorService pool = Executors.newFixedThreadPool(2);

    Callable<Long> callable1 = () -> {
      Long removedCount = 0L;
      for (int i = 0; i < 1000; i++) {
        try {
          Long result = jedis.srem("set", "member");
          startCyclicBarrier.await();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      return removedCount;
    };

    Callable<Long> callable2 = () -> {
      Long addedCount = 0L;
      for (int i = 0; i < 1000; i++) {
        try {
          addedCount += jedis2.sadd("set", "member");
          startCyclicBarrier.await();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      return addedCount;
    };

    Future<Long> future1 = pool.submit(callable1);
    Future<Long> future2 = pool.submit(callable2);

    future1.get();
    future2.get();

    assertThat(errorCount.get())
        .as("Inconsistency between keyRegistrar and backing store detected.").isEqualTo(0L);
  }

  @Test
  public void testConcurrentSRems() throws InterruptedException {
    int ENTRIES = 1000;

    List<String> masterSet = new ArrayList<>();
    for (int i = 0; i < ENTRIES; i++) {
      masterSet.add("master-" + i);
    }

    jedis.sadd("master", masterSet.toArray(new String[] {}));

    AtomicLong sremmed1 = new AtomicLong(0);
    Runnable runnable1 = () -> {
      for (int i = 0; i < ENTRIES; i++) {
        sremmed1.addAndGet(jedis.srem("master", masterSet.get(i)));
        Thread.yield();
      }
    };

    AtomicLong sremmed2 = new AtomicLong(0);
    Runnable runnable2 = () -> {
      for (int i = 0; i < ENTRIES; i++) {
        sremmed2.addAndGet(jedis2.srem("master", masterSet.get(i)));
        Thread.yield();
      }
    };

    Thread thread1 = new Thread(runnable1);
    Thread thread2 = new Thread(runnable2);

    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();

    assertThat(jedis.smembers("master")).isEmpty();
    assertThat(sremmed1.get() + sremmed2.get()).isEqualTo(ENTRIES);
  }
}
