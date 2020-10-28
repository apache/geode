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

package org.apache.geode.redis.internal.executor.connection;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.TestRule;
import org.testcontainers.containers.GenericContainer;
import redis.clients.jedis.Jedis;

import org.apache.geode.test.junit.rules.IgnoreOnWindowsRule;

public class SelectNativeRedisAcceptanceTest extends AbstractSelectIntegrationTest {

  // Docker compose does not work on windows in CI. Ignore this test on windows
  // Using a RuleChain to make sure we ignore the test before the rule comes into play
  @ClassRule
  public static TestRule ignoreOnWindowsRule = new IgnoreOnWindowsRule();

  private GenericContainer<?> redisContainer;

  @Before
  public void setUp() {
    // starting redis in cluster mode
    redisContainer =
        new GenericContainer<>("grokzen/redis-cluster:5.0.9")
            .withExposedPorts(7000)
            .withEnv("IP", "0.0.0.0");

    redisContainer.start();
    jedis = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    jedis.close();
    redisContainer.stop();
  }

  @Override
  public int getPort() {
    return redisContainer.getFirstMappedPort();
  }

}
