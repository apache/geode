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

import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Rule;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

public class GeodeRedisServerGfshStartupAcceptanceTest {

  @Rule
  public GfshRule gfsh = new GfshRule();

  @Test
  public void gfshStartsRedisServer_whenRedisEnabled() {
    String command =
        "start server --J=-Dgemfire." + ConfigurationProperties.GEODE_FOR_REDIS_ENABLED + "=true";
    gfsh.execute(command);

    try (Jedis jedis = new Jedis(BIND_ADDRESS, 6379)) {
      assertThat(jedis.ping()).isEqualTo("PONG");
    }
  }

  @Test
  public void gfshStartsRedisServer_whenCustomPort() {
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    String command =
        "start server --J=-Dgemfire." + ConfigurationProperties.GEODE_FOR_REDIS_ENABLED + "=true"
            + " --J=-Dgemfire." + ConfigurationProperties.GEODE_FOR_REDIS_PORT + "=" + port;

    gfsh.execute(command);

    try (Jedis jedis = new Jedis(BIND_ADDRESS, port)) {
      assertThat(jedis.ping()).isEqualTo("PONG");
    }
  }

  @Test
  public void gfshStartsRedisServer_whenCustomPortAndBindAddress() {
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    String anyLocal = LocalHostUtil.getAnyLocalAddress().getHostAddress();
    String command =
        "start server --J=-Dgemfire." + ConfigurationProperties.GEODE_FOR_REDIS_ENABLED + "=true"
            + " --J=-Dgemfire." + ConfigurationProperties.GEODE_FOR_REDIS_PORT + "=" + port
            + " --J=-Dgemfire." + ConfigurationProperties.GEODE_FOR_REDIS_BIND_ADDRESS + "="
            + anyLocal;

    gfsh.execute(command);

    try (Jedis jedis = new Jedis(anyLocal, port)) {
      assertThat(jedis.ping()).isEqualTo("PONG");
    }
  }

  @Test
  public void gfshDoesNotStartRedisServer_whenNotRedisEnabled() {
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    String anyLocal = LocalHostUtil.getAnyLocalAddress().getHostAddress();
    String command =
        "start server --J=-Dgemfire." + ConfigurationProperties.GEODE_FOR_REDIS_PORT + "=" + port
            + " --J=-Dgemfire." + ConfigurationProperties.GEODE_FOR_REDIS_BIND_ADDRESS + "="
            + anyLocal;

    gfsh.execute(command);

    try (Jedis jedis = new Jedis(BIND_ADDRESS, port)) {
      assertThatThrownBy(() -> jedis.ping()).isInstanceOf(JedisConnectionException.class);
    }
  }

}
