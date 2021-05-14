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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class OutOfMemoryDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(4);

  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  private static final String expectedEx = "Member: .*? above .*? critical threshold";
  private static final String FILLER_KEY = "fillerKey-";
  private static final String PRESSURE_KEY = "pressureKey-";
  private static final String LOCAL_HOST = "127.0.0.1";
  private static final int KEY_TTL_SECONDS = 10;
  private static final int MAX_ITERATION_COUNT = 4000;
  private static final int LARGE_VALUE_SIZE = 128 * 1024;
  private static final int PRESSURE_VALUE_SIZE = 4 * 1024;
  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  private static Jedis jedis1;
  private static Jedis jedis2;

  private static MemberVM server1;
  private static MemberVM server2;

  private static Thread memoryPressureThread;
  private static int redisServerPort1;

  @BeforeClass
  public static void classSetup() {
    IgnoredException.addIgnoredException(expectedEx);

    MemberVM locator = clusterStartUp.startLocatorVM(0);

    Properties serverProperties = new Properties();
    server1 = clusterStartUp.startRedisVM(1, serverProperties, locator.getPort());
    server2 = clusterStartUp.startRedisVM(2, serverProperties, locator.getPort());

    server1.getVM().invoke(() -> RedisClusterStartupRule.getCache().getResourceManager()
        .setCriticalHeapPercentage(5.0F));
    server2.getVM().invoke(() -> RedisClusterStartupRule.getCache().getResourceManager()
        .setCriticalHeapPercentage(5.0F));

    redisServerPort1 = clusterStartUp.getRedisPort(1);
    int redisServerPort2 = clusterStartUp.getRedisPort(2);

    jedis1 = new Jedis(LOCAL_HOST, redisServerPort1, JEDIS_TIMEOUT);
    jedis2 = new Jedis(LOCAL_HOST, redisServerPort2, JEDIS_TIMEOUT);
  }

  @Before
  public void testSetup() {
    clusterStartUp.flushAll(redisServerPort1);
  }

  @AfterClass
  public static void tearDown() {
    jedis1.disconnect();
    jedis2.disconnect();

    server1.stop();
    server2.stop();
  }

  @Test
  public void shouldReturnOOMError_forWriteOperations_whenThresholdReached()
      throws InterruptedException {
    IgnoredException.addIgnoredException(expectedEx);
    IgnoredException.addIgnoredException("LowMemoryException");

    memoryPressureThread = new Thread(makeMemoryPressureRunnable());
    memoryPressureThread.start();

    fillMemory(jedis2, false);

    assertThatThrownBy(() -> jedis2.set("oneMoreKey", makeLongStringValue(2 * LARGE_VALUE_SIZE)))
        .hasMessageContaining("OOM");

    memoryPressureThread.interrupt();
    memoryPressureThread.join();
  }

  @Test
  public void shouldAllowDeleteOperations_afterThresholdReached() throws InterruptedException {
    IgnoredException.addIgnoredException(expectedEx);
    IgnoredException.addIgnoredException("LowMemoryException");

    memoryPressureThread = new Thread(makeMemoryPressureRunnable());
    memoryPressureThread.start();

    fillMemory(jedis2, false);

    assertThatNoException().isThrownBy(() -> jedis2.del(FILLER_KEY + 1));

    memoryPressureThread.interrupt();
    memoryPressureThread.join();
  }

  @Test
  public void shouldAllowExpiration_afterThresholdReached() throws InterruptedException {
    IgnoredException.addIgnoredException(expectedEx);
    IgnoredException.addIgnoredException("LowMemoryException");

    memoryPressureThread = new Thread(makeMemoryPressureRunnable());
    memoryPressureThread.start();

    fillMemory(jedis2, true);

    await().untilAsserted(() -> {
      assertThat(jedis2.ttl(FILLER_KEY + 1)).isEqualTo(-2);
    });

    memoryPressureThread.interrupt();
    memoryPressureThread.join();
  }

  // TODO: test that write operations become allowed after memory has dropped
  // below critical levels. Difficult to do right now because of vagaries of the
  // Java garbage collector.

  private void fillMemory(Jedis jedis, boolean withExpiration) {
    String valueString;
    int valueSize = LARGE_VALUE_SIZE;

    while (valueSize > 1) {
      forceGC(); // Helps ensure we really do fill all available memory
      valueString = makeLongStringValue(LARGE_VALUE_SIZE);
      addMultipleKeys(jedis, valueString, withExpiration);
      valueSize /= 2;
    }
  }

  private void addMultipleKeys(Jedis jedis, String valueString, boolean withExpiration) {
    // count is final because it is never reassigned
    AtomicInteger count = new AtomicInteger();

    Throwable thrown = catchThrowable(() -> {
      for (count.set(0); count.get() < MAX_ITERATION_COUNT; count.incrementAndGet()) {
        setRedisKeyAndValue(jedis, withExpiration, valueString, count.get());
      }
    });

    assertThat(thrown)
        .isInstanceOf(Exception.class)
        .hasMessageContaining("OOM command not allowed");

    assertThat(count.get()).isLessThan(MAX_ITERATION_COUNT);
  }

  private void setRedisKeyAndValue(Jedis jedis, boolean withExpiration, String valueString,
      int keyNumber) {
    if (withExpiration) {
      jedis.setex(FILLER_KEY + keyNumber, KEY_TTL_SECONDS, valueString);
    } else {
      jedis.set(FILLER_KEY + keyNumber, valueString);
    }
  }

  private static String makeLongStringValue(int requestedSize) {
    char[] largeCharData = new char[requestedSize];
    Arrays.fill(largeCharData, 'a');
    return new String(largeCharData);
  }

  private static Runnable makeMemoryPressureRunnable() {
    return new Runnable() {
      boolean running = true;
      String pressureValue = makeLongStringValue(PRESSURE_VALUE_SIZE);

      @Override
      public void run() {
        int i = 0;
        while (running) {
          if (Thread.currentThread().isInterrupted()) {
            running = false;
            break;
          }
          try {
            jedis1.set(PRESSURE_KEY + i, pressureValue);
          } catch (JedisException je) {
            // Ignore, keep trying to fill memory
          }
          i++;
        }
      }
    };
  }

  private void forceGC() {
    server1.getVM().invoke(() -> {
      Runtime.getRuntime().gc();
    });
    server2.getVM().invoke(() -> {
      Runtime.getRuntime().gc();
    });
  }
}
