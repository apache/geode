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
package org.apache.geode.redis.internal;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.redis.GeodeRedisServerRule;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public class RedisStatsIntegrationTest {

  @ClassRule
  public static GeodeRedisServerRule server = new GeodeRedisServerRule();

  @Test
  public void clientsStat_withConnectAndClose_isCorrect() throws InterruptedException {
    long initialClients = server.getServer().getStats().getClients();
    Jedis jedis = new Jedis("localhost", server.getPort(), 10000000);

    jedis.ping();
    assertThat(server.getServer().getStats().getClients()).isEqualTo(initialClients + 1);

    jedis.close();
    GeodeAwaitility.await().untilAsserted(
        () -> assertThat(server.getServer().getStats().getClients()).isEqualTo(initialClients));
  }
}
