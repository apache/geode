/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.modules;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Set;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class RedisAcceptanceTest extends AbstractDockerizedAcceptanceTest {

  private StatefulRedisConnection<String, String> connection;
  private RedisClient redisClient;

  public RedisAcceptanceTest(String launchCommand) throws IOException, InterruptedException {
    launch(launchCommand);
  }

  @Before
  public void setup() {
    redisClient = RedisClient.create(RedisURI.Builder.redis(host, redisPort).build());
    connection = redisClient.connect();
  }

  @After
  public void tearDown() {
    connection.close();
    redisClient.shutdown();
  }

  @Test
  public void testRedisConnected() {
    assertThat(connection.isOpen()).isTrue();
  }

  @Test
  public void testRedisCommands() {
    RedisCommands<String, String> redisCommands = connection.sync();
    Long pushedValuesCount = redisCommands.sadd("testList", "value1", "value2");
    assertThat(pushedValuesCount).isEqualTo(2);

    Set<String> members = redisCommands.smembers("testList");
    assertThat(members).contains("value1", "value2");
    redisCommands.srem("testList", "value1", "value2");
  }
}
