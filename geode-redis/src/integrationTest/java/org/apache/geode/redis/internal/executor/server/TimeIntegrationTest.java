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

package org.apache.geode.redis.internal.executor.server;

import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.redis.GeodeRedisServerRule;

public class TimeIntegrationTest {

  public Jedis jedis;
  public static int REDIS_CLIENT_TIMEOUT = 10000;

  @Rule
  public GeodeRedisServerRule server = new GeodeRedisServerRule()
      .withProperty(LOG_LEVEL, "info");

  @Before
  public void setUp() {
    jedis = new Jedis("localhost", server.getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void classLevelTearDown() {
    jedis.close();
  }

  @Test
  public void timeCommandRespondsWIthTwoValues() {
    List<String> timestamp = jedis.time();

    assertThat(timestamp).hasSize(2);
    assertThat(Long.parseLong(timestamp.get(0))).isGreaterThan(0);
    assertThat(Long.parseLong(timestamp.get(1))).isGreaterThan(0);
  }

}
