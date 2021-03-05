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

import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Properties;

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

@SuppressWarnings("unchecked")
public class OutOfMemoryDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(4);

  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  private static final String expectedEx = "Member: .*? above .*? critical threshold";
  public static final String FILLER_KEY = "fillerKey-";
  private static final String LOCAL_HOST = "127.0.0.1";
  public static final int KEY_TTL_SECONDS = 10;
  private static final int MAX_ITERATION_COUNT = 4000;
  public static final int VALUE_SIZE = 128 * 1024;
  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  private static Jedis jedis1;
  private static Jedis jedis2;

  private static Properties locatorProperties;
  private static Properties serverProperties;

  private static MemberVM locator;
  private static MemberVM server1;
  private static MemberVM server2;

  private static int redisServerPort1;
  private static int redisServerPort2;
  private static String valueString = "";

  @BeforeClass
  public static void classSetup() {
    IgnoredException.addIgnoredException(expectedEx);
    locatorProperties = new Properties();
    serverProperties = new Properties();
    locatorProperties.setProperty(MAX_WAIT_TIME_RECONNECT, "15000");

    locator = clusterStartUp.startLocatorVM(0, locatorProperties);
    server1 = clusterStartUp.startRedisVM(1, serverProperties, locator.getPort());
    server2 = clusterStartUp.startRedisVM(2, serverProperties, locator.getPort());

    server1.getVM().invoke(() -> {
      RedisClusterStartupRule.getCache().getResourceManager().setCriticalHeapPercentage(5.0F);
    });
    server2.getVM().invoke(() -> {
      RedisClusterStartupRule.getCache().getResourceManager().setCriticalHeapPercentage(5.0F);
    });

    redisServerPort1 = clusterStartUp.getRedisPort(1);
    redisServerPort2 = clusterStartUp.getRedisPort(2);

    jedis1 = new Jedis(LOCAL_HOST, redisServerPort1, JEDIS_TIMEOUT);
    jedis2 = new Jedis(LOCAL_HOST, redisServerPort2, JEDIS_TIMEOUT);

    char[] largeCharData = new char[VALUE_SIZE];
    Arrays.fill(largeCharData, 'a');
    valueString = new String(largeCharData);

  }

  @Before
  public void testSetup() {
    jedis1.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedis1.disconnect();
    jedis2.disconnect();

    server1.stop();
    server2.stop();
  }

  @Test
  public void shouldReturnOOMError_forWriteOperations_whenThresholdReached() {
    IgnoredException.addIgnoredException(expectedEx);
    IgnoredException.addIgnoredException("LowMemoryException");

    fillMemory(jedis1, MAX_ITERATION_COUNT, false);

    assertThatThrownBy(() -> jedis2.set("oneMoreKey", valueString)).hasMessageContaining("OOM");
  }

  @Test
  public void shouldAllowDeleteOperations_afterThresholdReached() {
    IgnoredException.addIgnoredException(expectedEx);
    IgnoredException.addIgnoredException("LowMemoryException");

    fillMemory(jedis1, MAX_ITERATION_COUNT, false);

    assertThatNoException().isThrownBy(() -> jedis2.del(FILLER_KEY + 1));
  }

  @Test
  public void shouldAllowExpiration_afterThresholdReached() {
    IgnoredException.addIgnoredException(expectedEx);
    IgnoredException.addIgnoredException("LowMemoryException");

    fillMemory(jedis1, MAX_ITERATION_COUNT, true);

    GeodeAwaitility.await().until(() -> jedis2.ttl(FILLER_KEY + 1) == -2);
  }

  // TODO: test that write operations become allowed after memory has dropped
  // below critical levels. Difficult to do right now because of vagaries of the
  // Java garbage collector.

  private int fillMemory(Jedis jedis, int maxIterations, boolean withExpiration) {
    int i = 0;
    while (i < maxIterations) {
      try {
        if (withExpiration) {
          jedis.setex(FILLER_KEY + i, KEY_TTL_SECONDS, valueString);
        } else {
          jedis.set(FILLER_KEY + i, valueString);
        }
      } catch (JedisException je) {
        assertThat(je).hasMessageContaining("OOM command not allowed");
        break;
      }
      i++;
    }
    assertThat(i).isLessThan(maxIterations);
    return i;
  }

  // TODO: use this when write testing figured out
  private void deleteKeysToClearMemory(Jedis jedis, int keysToDelete) {
    for (int i = 0; i < keysToDelete; i++) {
      assertThat(jedis.del(FILLER_KEY + i)).isEqualTo(1);
    }
  }
}
