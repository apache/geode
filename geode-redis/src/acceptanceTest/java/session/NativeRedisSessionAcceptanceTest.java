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
package session;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.testcontainers.containers.GenericContainer;

import org.apache.geode.redis.session.RedisSessionDUnitTest;
import org.apache.geode.test.junit.rules.IgnoreOnWindowsRule;

public class NativeRedisSessionAcceptanceTest extends RedisSessionDUnitTest {

  @ClassRule
  public static TestRule ignoreOnWindowsRule = new IgnoreOnWindowsRule();

  @BeforeClass
  public static void setup() {
    setupAppPorts();
    setupNativeRedis();
    setupClient();
    startSpringApp(APP1, DEFAULT_SESSION_TIMEOUT, ports.get(SERVER1));
    startSpringApp(APP2, DEFAULT_SESSION_TIMEOUT, ports.get(SERVER1));
  }

  protected static void setupNativeRedis() {
    GenericContainer redisContainer = new GenericContainer<>("redis:5.0.6").withExposedPorts(6379);
    redisContainer.start();
    ports.put(SERVER1, redisContainer.getFirstMappedPort());
  }

  @Test
  public void should_getSessionFromServer1_whenServer2GoesDown() {
    // Only using one server for Native Redis
  }

  @Test
  public void should_getSessionFromServer_whenServerGoesDownAndIsRestarted() {
    // Only using one server for Native Redis
  }

  @Test
  public void should_getSession_whenServer2GoesDown_andAppFailsOverToServer1() {
    // Only using one server for Native Redis
  }
}
