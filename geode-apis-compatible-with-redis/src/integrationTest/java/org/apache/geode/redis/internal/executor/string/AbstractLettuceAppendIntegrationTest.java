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

package org.apache.geode.redis.internal.executor.string;

import static org.assertj.core.api.Assertions.assertThat;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public abstract class AbstractLettuceAppendIntegrationTest implements RedisIntegrationTest {

  protected RedisClient client;

  @ClassRule
  public static ExecutorServiceRule executor = new ExecutorServiceRule();

  @Before
  public void before() {
    client = RedisClient.create("redis://localhost:" + getPort());
  }

  @After
  public void after() {
    client.shutdown();
  }

  @Test
  public void testAppend_withUTF16KeyAndValue() {
    String test_utf16_string = "最𐐷𤭢";
    String double_utf16_string = test_utf16_string + test_utf16_string;

    StatefulRedisConnection<String, String> redisConnection = client.connect();
    RedisCommands<String, String> syncCommands = redisConnection.sync();

    syncCommands.set(test_utf16_string, test_utf16_string);
    syncCommands.append(test_utf16_string, test_utf16_string);
    String result = syncCommands.get(test_utf16_string);
    assertThat(result).isEqualTo(double_utf16_string);
  }
}
