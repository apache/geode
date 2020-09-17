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

package org.apache.geode.redis.internal.executor.server;


import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.redis.GeodeRedisServerRule;


public class InfoIntegrationTest {
  public static Jedis jedis;

  public static int REDIS_CLIENT_TIMEOUT = 10000;

  @ClassRule
  public static GeodeRedisServerRule server = new GeodeRedisServerRule()
      .withProperty(LOG_LEVEL, "info");

  @BeforeClass
  public static void setUp() {
    jedis = new Jedis("localhost", server.getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();
  }

  protected int getTCPPort() {
    return server.getPort();
  }

  @Test
  public void shouldReturnRedisVersion() {
    final String EXPECTED_RESULT = "redis_version:5.0.6";

    String actualResult = jedis.info();

    assertThat(actualResult).contains(EXPECTED_RESULT);
  }

  @Test
  public void shouldReturnTCPPort() {
    final int EXPECTED_PORT = getTCPPort();
    final String EXPECTED_RESULT = "tcp_port:" + EXPECTED_PORT;

    String actualResult = jedis.info();

    assertThat(actualResult).contains(EXPECTED_RESULT);
  }

  @Test
  public void shouldReturnRedisMode() {
    final String EXPECTED_RESULT = "redis_mode:standalone";

    String actualResult = jedis.info();

    assertThat(actualResult).contains(EXPECTED_RESULT);
  }

  @Test
  public void shouldReturnLoadingProperty() {
    final String EXPECTED_RESULT = "loading:0";

    String actualResult = jedis.info();

    assertThat(actualResult).contains(EXPECTED_RESULT);
  }

  @Test
  public void shouldReturnClusterEnabledProperty() {
    final String EXPECTED_RESULT = "cluster_enabled:0";

    String actualResult = jedis.info();

    assertThat(actualResult).contains(EXPECTED_RESULT);
  }
}
