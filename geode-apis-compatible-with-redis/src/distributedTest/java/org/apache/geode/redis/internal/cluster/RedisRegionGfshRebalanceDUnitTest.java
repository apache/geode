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
package org.apache.geode.redis.internal.cluster;

import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class RedisRegionGfshRebalanceDUnitTest {
  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(4);

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  private static final String LOCAL_HOST = "127.0.0.1";
  private static final int JEDIS_TIMEOUT = Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  private static final int SET_SIZE = 10000;
  private static Jedis jedis1;

  private static MemberVM locator;
  private static MemberVM server1;
  private static MemberVM server2;
  private static MemberVM server3;

  private static Properties locatorProperties;

  private static int redisServerPort1;

  @BeforeClass
  public static void classSetup() {
    locatorProperties = new Properties();
    locatorProperties.setProperty(MAX_WAIT_TIME_RECONNECT, "15000");
    final String buildDir = System.getProperty("geode.build.dir", System.getProperty("user.dir"));

    locator =
        clusterStartUp.startLocatorVM(0, s -> s.withHttpService()
            /* .withSystemProperty("geode.build.dir", buildDir) */.withProperties(
                locatorProperties));
    server1 = clusterStartUp.startRedisVM(1, locator.getPort());
    server2 = clusterStartUp.startRedisVM(2, locator.getPort());
    server3 = clusterStartUp.startRedisVM(3, locator.getPort());

    redisServerPort1 = clusterStartUp.getRedisPort(1);

    jedis1 = new Jedis(LOCAL_HOST, redisServerPort1, JEDIS_TIMEOUT);
  }

  @AfterClass
  public static void tearDown() {
    jedis1.disconnect();
    server1.stop();
    server2.stop();
    locator.stop();
  }

  @Test
  public void whenGfshRebalanceCommandIsExecutedThenRedisRegionsAreRebalanced() throws Exception {
    List<String> members = makeMemberList(SET_SIZE, "member1-");
    jedis1.sadd("key", members.toArray(new String[] {}));
    Set<String> result = jedis1.smembers("key");

    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat("rebalance").statusIsSuccess()
        .containsOutput("Rebalanced partition regions /__REDIS_DATA");

    GeodeAwaitility.await()
        .untilAsserted(() -> assertThat(gfsh.execute("rebalance"))
            .contains("Rebalanced partition regions /__REDIS_DATA"));
  }

  private List<String> makeMemberList(int setSize, String baseString) {
    List<String> members = new ArrayList<>();
    for (int i = 0; i < setSize; i++) {
      members.add(baseString + i);
    }
    return members;
  }


}
