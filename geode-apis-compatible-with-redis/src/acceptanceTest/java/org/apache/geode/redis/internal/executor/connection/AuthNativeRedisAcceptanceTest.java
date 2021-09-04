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
package org.apache.geode.redis.internal.executor.connection;


import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.testcontainers.containers.GenericContainer;
import redis.clients.jedis.Jedis;

import org.apache.geode.test.junit.rules.IgnoreOnWindowsRule;

public class AuthNativeRedisAcceptanceTest extends AbstractAuthIntegrationTest {

  private static final String REDIS_DOCKER_IMAGE = "redis:6.2.4";

  // Docker compose does not work on windows in CI. Ignore this test on windows
  // Using a RuleChain to make sure we ignore the test before the rule comes into play
  @ClassRule
  public static TestRule ignoreOnWindowsRule = new IgnoreOnWindowsRule();

  private GenericContainer<?> redisContainer;

  @After
  public void tearDown() {
    jedis.close();
    redisContainer.stop();
  }

  @Override
  public void setupCacheWithSecurity(boolean withSecurityManager) {
    redisContainer =
        new GenericContainer<>(REDIS_DOCKER_IMAGE).withExposedPorts(6379)
            .withCommand("redis-server --requirepass " + PASSWORD);
    redisContainer.start();
    jedis = new Jedis("localhost", redisContainer.getFirstMappedPort(), REDIS_CLIENT_TIMEOUT);
  }

  @Override
  public void setupCacheWithoutSecurity() {
    redisContainer = new GenericContainer<>(REDIS_DOCKER_IMAGE).withExposedPorts(6379);
    redisContainer.start();
    jedis = new Jedis("localhost", redisContainer.getFirstMappedPort(), REDIS_CLIENT_TIMEOUT);
  }

  @Override
  public int getPort() {
    return redisContainer.getFirstMappedPort();
  }

  @Override
  @Test
  public void givenNoSecurity_lettuceV6AuthClient_defaultUsernameAndAnyPassword_passes()
      throws Exception {
    setupCacheWithoutSecurity();

    // Implicitly sends the default username ('default') as part of the AUTH request
    RedisURI uri =
        RedisURI.create(String.format("redis://%s@localhost:%d", "not-default", getPort()));
    RedisClient client = RedisClient.create(uri);

    client.connect().sync().ping();
  }
}
