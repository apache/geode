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

package org.apache.geode.redis;

import static org.apache.geode.distributed.ConfigurationProperties.REDIS_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.redis.internal.GeodeRedisService;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class GeodeRedisServerStartupDUnitTest {

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Test
  public void startupOnDefaultPort() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withProperty(REDIS_PORT, "6379")
        .withProperty(REDIS_BIND_ADDRESS, "localhost"));

    server.invoke(() -> {
      GeodeRedisService service = ClusterStartupRule.getCache().getService(GeodeRedisService.class);
      assertThat(service.getPort()).isEqualTo(GeodeRedisServer.DEFAULT_REDIS_SERVER_PORT);
    });
  }

  @Test
  public void startupOnRandomPort_whenPortIsNegativeOne() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withProperty(REDIS_PORT, "-1")
        .withProperty(REDIS_BIND_ADDRESS, "localhost"));

    server.invoke(() -> {
      GeodeRedisService service = ClusterStartupRule.getCache().getService(GeodeRedisService.class);
      assertThat(service.getPort()).isNotEqualTo(GeodeRedisServer.DEFAULT_REDIS_SERVER_PORT);
    });
  }

  @Test
  public void doNotStartup_whenPortIsZero() {
    MemberVM server = cluster.startServerVM(0, s -> s
        .withProperty(REDIS_PORT, "0")
        .withProperty(REDIS_BIND_ADDRESS, "localhost"));

    server.invoke(() -> {
      assertThat(ClusterStartupRule.getCache().getService(GeodeRedisService.class))
          .as("GeodeRedisService should not exist")
          .isNull();
    });
  }

}
