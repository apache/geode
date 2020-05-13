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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Rule;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.redis.internal.GeodeRedisServer;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public class RedisUsePersistentRegionDUnitTest {

  @Rule
  public final ServerStarterRule server = new ServerStarterRule();

  @Test
  public void startRedisWithPersistentRegion() throws Exception {
    int port = AvailablePortHelper.getRandomAvailableTCPPort();

    ServerStarterRule server = new ServerStarterRule()
        .withJMXManager()
        .withProperty("redis-port", Integer.toString(port))
        .withProperty("redis-bind-address", "localhost")
        .withSystemProperty(GeodeRedisServer.DEFAULT_REGION_SYS_PROP_NAME, "PARTITION_PERSISTENT")
        .withAutoStart();;

    // Using before() ensures that system properties are applied correctly
    server.before();

    Jedis client = new Jedis("localhost", port);

    long result = client.hset("user", "name", "Joe");
    assertThat(result).isEqualTo(1);

    Region<?, ?> stringRegion = server.getCache().getRegion(GeodeRedisServer.STRING_REGION);
    assertThat(stringRegion.getAttributes().getDataPolicy().withPersistence()).isTrue();
  }
}
