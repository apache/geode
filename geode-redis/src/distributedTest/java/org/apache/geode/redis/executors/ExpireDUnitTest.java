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

import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.GeodeRedisService;
import org.apache.geode.redis.internal.executor.TTLExecutor;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

@Ignore("GEODE-8058: this test needs to pass to have feature parity with native redis")
public class ExpireDUnitTest {

  @ClassRule
  public static ClusterStartupRule clusterStartUp = new ClusterStartupRule(4);

  static final String LOCAL_HOST = "127.0.0.1";
  static int[] availablePorts;
  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  static Jedis jedis1;
  static Jedis jedis2;
  static Jedis jedis3;

  static Properties locatorProperties;
  static Properties serverProperties1;
  static Properties serverProperties2;
  static Properties serverProperties3;

  static MemberVM locator;
  static MemberVM server1;
  static MemberVM server2;
  static MemberVM server3;

  @BeforeClass
  public static void classSetup() {

    availablePorts = AvailablePortHelper.getRandomAvailableTCPPorts(3);

    locatorProperties = new Properties();
    serverProperties1 = new Properties();
    serverProperties2 = new Properties();
    serverProperties3 = new Properties();

    locatorProperties.setProperty(MAX_WAIT_TIME_RECONNECT, "15000");

    serverProperties1.setProperty(REDIS_PORT, Integer.toString(availablePorts[0]));
    serverProperties1.setProperty(REDIS_BIND_ADDRESS, LOCAL_HOST);

    serverProperties2.setProperty(REDIS_PORT, Integer.toString(availablePorts[1]));
    serverProperties2.setProperty(REDIS_BIND_ADDRESS, LOCAL_HOST);

    serverProperties3.setProperty(REDIS_PORT, Integer.toString(availablePorts[2]));
    serverProperties3.setProperty(REDIS_BIND_ADDRESS, LOCAL_HOST);

    locator = clusterStartUp.startLocatorVM(0, locatorProperties);
    server1 = clusterStartUp.startServerVM(1, serverProperties1, locator.getPort());
    server2 = clusterStartUp.startServerVM(2, serverProperties2, locator.getPort());
    server3 = clusterStartUp.startServerVM(3, serverProperties3, locator.getPort());

    jedis1 = new Jedis(LOCAL_HOST, availablePorts[0], JEDIS_TIMEOUT);
    jedis2 = new Jedis(LOCAL_HOST, availablePorts[1], JEDIS_TIMEOUT);
    jedis3 = new Jedis(LOCAL_HOST, availablePorts[2], JEDIS_TIMEOUT);
  }

  @After
  public void testCleanUp() {
    jedis1.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedis1.disconnect();
    jedis2.disconnect();
    jedis3.disconnect();

    server1.stop();
    server2.stop();
    server3.stop();
  }

  @Test
  public void expireOnOneServer_shouldPropagateToAllServers() {
    String key = "key";

    jedis1.set(key, "value");
    jedis1.expire(key, 20);

    assertThat(jedis2.ttl(key)).isGreaterThan(0);
  }

  @Test
  public void expireOnOneServer_shouldResultInKeyRemovalFromOtherServer() {
    String key = "key";

    jedis1.set(key, "value");
    jedis1.expire(key, 2);

    assertThat(jedis2.ttl(key)).isGreaterThan(0);
    GeodeAwaitility.await().until(() -> jedis2.ttl(key) == TTLExecutor.NOT_EXISTS);
  }

  @Test
  public void whenExpirationIsSet_andIsUpdatedOnAnotherServer_itIsReflectedOnFirstServer() {
    String key = "key";

    jedis1.set(key, "value");
    jedis1.expire(key, 20);
    jedis2.expire(key, 10000);

    assertThat(jedis1.ttl(key)).isGreaterThan(20L);
  }

  @Test
  public void whenExpirationIsSet_andKeyIsResetOnAnotherServer_ttlIsRemovedOnFirstServer() {
    String key = "key";

    jedis1.set(key, "value");
    jedis1.expire(key, 20);
    jedis2.set(key, "newValue");

    assertThat(jedis1.ttl(key)).isEqualTo(TTLExecutor.NO_TIMEOUT);
  }

  @Test
  public void whenExpirationIsSet_andKeyIsPersistedOnAnotherServer_ttlIsRemovedOnFirstServer() {
    String key = "key";

    jedis1.set(key, "value");
    jedis1.expire(key, 20);
    jedis2.persist(key);

    assertThat(jedis1.ttl(key)).isEqualTo(TTLExecutor.NO_TIMEOUT);
  }

  @Test
  public void whenExpirationIsSet_andKeyIsDeletedOnAnotherServer_ttlReflectsChanges() {
    String key = "key";

    jedis1.set(key, "value");
    jedis1.expire(key, 20);
    jedis2.del(key);

    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      GeodeRedisService redisService = cache.getService(GeodeRedisService.class);
      boolean hasExpiration = redisService.getGeodeRedisServer().getRegionCache()
          .hasExpiration(new ByteArrayWrapper(key.getBytes()));
      assertThat(hasExpiration).as("expiration should not be set").isFalse();
    });

    assertThat(jedis1.ttl(key)).isEqualTo(TTLExecutor.NOT_EXISTS);
  }


  @Test
  public void whenExpirationIsSet_andKeyWithoutExpirationIsRenamedOnAnotherServer_expirationIsCorrectlyTransferred() {
    String key1 = "key1";
    String key2 = "key2";

    jedis1.set(key1, "value");
    jedis1.set(key2, "value");
    jedis1.expire(key1, 20);
    jedis2.rename(key1, key2);

    assertThat(jedis1.ttl(key2)).isGreaterThan(0L);
    assertThat(jedis2.ttl(key2)).isGreaterThan(0L);
  }

  @Test
  public void pExpireOnOneServer_shouldResultInKeyRemovalFromOtherServer() {
    String key = "key";

    jedis1.set(key, "value");
    jedis1.pexpire(key, 2000);

    assertThat(jedis2.ttl(key)).isGreaterThan(0);
    GeodeAwaitility.await().until(() -> jedis2.ttl(key) == TTLExecutor.NOT_EXISTS);
  }

  @Test
  public void expireAtOnOneServer_shouldResultInKeyRemovalFromOtherServer() {
    String key = "key";

    jedis1.set(key, "value");
    jedis1.expireAt(key, System.currentTimeMillis() / 1000 + 2);

    assertThat(jedis2.ttl(key)).isGreaterThan(0);
    GeodeAwaitility.await().until(() -> jedis2.ttl(key) == TTLExecutor.NOT_EXISTS);
  }

  @Test
  public void pExpireAtOnOneServer_shouldResultInKeyRemovalFromOtherServer() {
    String key = "key";

    jedis1.set(key, "value");
    jedis1.pexpireAt(key, System.currentTimeMillis() + 2000);

    assertThat(jedis2.ttl(key)).isGreaterThan(0);
    GeodeAwaitility.await().until(() -> jedis2.ttl(key) == TTLExecutor.NOT_EXISTS);
  }
}
