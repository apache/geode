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
 *
 */


package org.apache.geode.redis.internal.data;

import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public class PartitionedRegionStatsUpdateTest {

  @ClassRule
  public static RedisClusterStartupRule clusterStartUpRule = new RedisClusterStartupRule(2);

  private static MemberVM server1;
  private static final String LOCAL_HOST = "127.0.0.1";
  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  private static Jedis jedis1;

  @BeforeClass
  public static void classSetup() {
    Properties locatorProperties = new Properties();
    locatorProperties.setProperty(MAX_WAIT_TIME_RECONNECT, "15000");

    MemberVM locator = clusterStartUpRule.startLocatorVM(0, locatorProperties);
    int locatorPort = locator.getPort();

    server1 = clusterStartUpRule.startRedisVM(1, locatorPort);
    int redisServerPort1 = clusterStartUpRule.getRedisPort(1);

    jedis1 = new Jedis(LOCAL_HOST, redisServerPort1, JEDIS_TIMEOUT);
  }

  @Test
  public void should_showIncreaseInDatastoreBytesInUse_givenValueSizeIncreases() {
    String KEY = "key";
    String LONG_APPEND_VALUE = String.valueOf(Integer.MAX_VALUE);
    jedis1.set(KEY, "value");

    long initialDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    for (int i = 0; i < 1000; i++) {
      jedis1.append(KEY, LONG_APPEND_VALUE);
    }

    long finalDataStoreBytesInUse = clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);
    assertThat(initialDataStoreBytesInUse).isLessThan(finalDataStoreBytesInUse);
  }
}
