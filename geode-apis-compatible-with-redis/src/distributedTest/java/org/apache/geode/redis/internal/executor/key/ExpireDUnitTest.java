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
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public class ExpireDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(4);

  static final String LOCAL_HOST = "127.0.0.1";
  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  private static JedisCluster jedis;

  private static Properties locatorProperties;

  private static MemberVM locator;
  private static MemberVM server1;
  private static MemberVM server2;
  private static MemberVM server3;

  private static int redisServerPort;

  @BeforeClass
  public static void classSetup() {

    locatorProperties = new Properties();
    locatorProperties.setProperty(MAX_WAIT_TIME_RECONNECT, "15000");

    locator = clusterStartUp.startLocatorVM(0, locatorProperties);
    server1 = clusterStartUp.startRedisVM(1, locator.getPort());
    server2 = clusterStartUp.startRedisVM(2, locator.getPort());
    server3 = clusterStartUp.startRedisVM(3, locator.getPort());

    redisServerPort = clusterStartUp.getRedisPort(1);


    jedis = new JedisCluster(new HostAndPort(LOCAL_HOST, redisServerPort), JEDIS_TIMEOUT);
  }

  @After
  public void testCleanUp() {
    try (Jedis connection = jedis.getConnectionFromSlot(0)) {
      connection.flushAll();
    }
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();

    server1.stop();
    server2.stop();
    server3.stop();
  }

  @Test
  public void expire_shouldPropagate() {
    String key = "key";

    jedis.sadd(key, "value");
    jedis.expire(key, 20);

    assertThat(jedis.ttl(key)).isGreaterThan(0);
  }

  @Test
  public void expire_shouldResultInKeyRemoval() {
    String key = "key";

    jedis.sadd(key, "value");
    jedis.expire(key, 1);

    GeodeAwaitility.await().until(() -> jedis.ttl(key) == -2);
  }

  @Test
  public void whenExpirationIsSet_andIsUpdated_itHasLastSetValue() {
    String key = "key";

    jedis.sadd(key, "value");
    jedis.expire(key, 20);
    jedis.expire(key, 10000);

    assertThat(jedis.ttl(key)).isGreaterThan(20L);
  }

  @Test
  public void whenExpirationIsSet_andKeyIsReset_ttlIsRemoved() {
    String key = "key";

    jedis.sadd(key, "value");
    jedis.expire(key, 20);
    jedis.del(key);
    jedis.sadd(key, "newValue");

    assertThat(jedis.ttl(key)).isEqualTo(-1);
  }

  @Test
  public void whenExpirationIsSet_andKeyIsPersisted_ttlIsRemoved() {
    String key = "key";

    jedis.sadd(key, "value");
    jedis.expire(key, 20);
    jedis.persist(key);

    assertThat(jedis.ttl(key)).isEqualTo(-1);
  }

  @Test
  public void whenExpirationIsSet_andKeyIsDeletedThenReset_ttlIsRemoved() {
    String key = "key";

    jedis.sadd(key, "value");
    jedis.expire(key, 10000);
    jedis.del(key);
    jedis.sadd(key, "newVal");

    assertThat(jedis.ttl(key)).isEqualTo(-1);
    assertThat(jedis.ttl(key)).isEqualTo(-1);
  }


  @Test
  public void whenExpirationIsSet_andKeyWithoutExpirationIsRenamed_expirationIsCorrectlySet() {
    String key1 = "{rename}key1";
    String key2 = "{rename}key2";

    jedis.sadd(key1, "value");
    jedis.sadd(key2, "value");
    jedis.expire(key1, 200);
    jedis.rename(key1, key2);

    assertThat(jedis.ttl(key2)).isGreaterThan(0L);
    assertThat(jedis.ttl(key2)).isGreaterThan(0L);
  }

  @Test
  public void pExpire_shouldResultInKeyRemoval() {
    String key = "key";

    jedis.sadd(key, "value");
    jedis.pexpire(key, 50);

    GeodeAwaitility.await().until(() -> jedis.ttl(key) == -2);
  }

  @Test
  public void expireAt_shouldResultInKeyRemoval() {
    String key = "key";

    jedis.sadd(key, "value");
    jedis.expireAt(key, System.currentTimeMillis() / 1000 + 2);

    GeodeAwaitility.await().until(() -> jedis.ttl(key) == -2);
  }

  @Test
  public void pExpireAt_shouldResultInKeyRemoval() {
    String key = "key";

    jedis.sadd(key, "value");
    jedis.pexpireAt(key, System.currentTimeMillis() + 100);

    GeodeAwaitility.await().until(() -> jedis.ttl(key) == -2);
  }
}
