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

import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public class ExpireDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(4);

  static final String LOCAL_HOST = "127.0.0.1";
  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  private static Jedis jedis1;
  private static Jedis jedis2;
  private static Jedis jedis3;

  private static Properties locatorProperties;

  private static MemberVM locator;
  private static MemberVM server1;
  private static MemberVM server2;
  private static MemberVM server3;

  private static int redisServerPort1;
  private static int redisServerPort2;
  private static int redisServerPort3;

  @BeforeClass
  public static void classSetup() {

    locatorProperties = new Properties();
    locatorProperties.setProperty(MAX_WAIT_TIME_RECONNECT, "15000");

    locator = clusterStartUp.startLocatorVM(0, locatorProperties);
    server1 = clusterStartUp.startRedisVM(1, locator.getPort());
    server2 = clusterStartUp.startRedisVM(2, locator.getPort());
    server3 = clusterStartUp.startRedisVM(3, locator.getPort());

    redisServerPort1 = clusterStartUp.getRedisPort(1);
    redisServerPort2 = clusterStartUp.getRedisPort(2);
    redisServerPort3 = clusterStartUp.getRedisPort(3);

    jedis1 = new Jedis(LOCAL_HOST, redisServerPort1, JEDIS_TIMEOUT);
    jedis2 = new Jedis(LOCAL_HOST, redisServerPort2, JEDIS_TIMEOUT);
    jedis3 = new Jedis(LOCAL_HOST, redisServerPort3, JEDIS_TIMEOUT);
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

    jedis1.sadd(key, "value");
    jedis1.expire(key, 20);

    assertThat(jedis2.ttl(key)).isGreaterThan(0);
  }

  @Test
  public void expireOnOneServer_shouldResultInKeyRemovalFromOtherServer() {
    String key = "key";

    jedis1.sadd(key, "value");
    jedis1.expire(key, 1);

    GeodeAwaitility.await().until(() -> jedis2.ttl(key) == -2);
  }

  @Test
  public void whenExpirationIsSet_andIsUpdatedOnAnotherServer_itIsReflectedOnFirstServer() {
    String key = "key";

    jedis1.sadd(key, "value");
    jedis1.expire(key, 20);
    jedis2.expire(key, 10000);

    assertThat(jedis1.ttl(key)).isGreaterThan(20L);
  }

  @Test
  public void whenExpirationIsSet_andKeyIsResetOnAnotherServer_ttlIsRemovedOnFirstServer() {
    String key = "key";

    jedis1.sadd(key, "value");
    jedis1.expire(key, 20);
    jedis2.del(key);
    jedis2.sadd(key, "newValue");

    assertThat(jedis1.ttl(key)).isEqualTo(-1);
  }

  @Test
  public void whenExpirationIsSet_andKeyIsPersistedOnAnotherServer_ttlIsRemovedOnFirstServer() {
    String key = "key";

    jedis1.sadd(key, "value");
    jedis1.expire(key, 20);
    jedis2.persist(key);

    assertThat(jedis1.ttl(key)).isEqualTo(-1);
  }

  @Test
  public void whenExpirationIsSet_andKeyIsDeletedOnAnotherServerThenReset_ttlIsRemoved() {
    String key = "key";

    jedis1.sadd(key, "value");
    jedis1.expire(key, 10000);
    jedis2.del(key);
    jedis2.sadd(key, "newVal");

    assertThat(jedis1.ttl(key)).isEqualTo(-1);
    assertThat(jedis2.ttl(key)).isEqualTo(-1);
  }


  @Test
  public void whenExpirationIsSet_andKeyWithoutExpirationIsRenamedOnAnotherServer_expirationIsCorrectlyTransferred() {
    String key1 = "key1";
    String key2 = "key2";

    jedis1.sadd(key1, "value");
    jedis1.sadd(key2, "value");
    jedis1.expire(key1, 200);
    jedis2.rename(key1, key2);

    assertThat(jedis1.ttl(key2)).isGreaterThan(0L);
    assertThat(jedis2.ttl(key2)).isGreaterThan(0L);
  }

  @Test
  public void pExpireOnOneServer_shouldResultInKeyRemovalFromOtherServer() {
    String key = "key";

    jedis1.sadd(key, "value");
    jedis1.pexpire(key, 50);

    GeodeAwaitility.await().until(() -> jedis2.ttl(key) == -2);
  }

  @Test
  public void expireAtOnOneServer_shouldResultInKeyRemovalFromOtherServer() {
    String key = "key";

    jedis1.sadd(key, "value");
    jedis1.expireAt(key, System.currentTimeMillis() / 1000 + 2);

    GeodeAwaitility.await().until(() -> jedis2.ttl(key) == -2);
  }

  @Test
  public void pExpireAtOnOneServer_shouldResultInKeyRemovalFromOtherServer() {
    String key = "key";

    jedis1.sadd(key, "value");
    jedis1.pexpireAt(key, System.currentTimeMillis() + 100);

    GeodeAwaitility.await().until(() -> jedis2.ttl(key) == -2);
  }
}
