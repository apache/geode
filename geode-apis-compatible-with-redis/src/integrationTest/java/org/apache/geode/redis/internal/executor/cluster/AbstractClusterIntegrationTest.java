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

package org.apache.geode.redis.internal.executor.cluster;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.test.dunit.rules.RedisPortSupplier;

public abstract class AbstractClusterIntegrationTest implements RedisPortSupplier {

  private JedisCluster jedis;

  @Before
  public void setup() {
    jedis = new JedisCluster(new HostAndPort("localhost", getPort()));
  }

  @After
  public void teardown() {
    jedis.close();
  }

  @Test
  public void testCluster_givenWrongNumberOfArguments() {
    assertThatThrownBy(() -> jedis.getConnectionFromSlot(0).sendCommand(Protocol.Command.CLUSTER))
        .hasMessage("ERR wrong number of arguments for 'cluster' command");
    assertThatThrownBy(
        () -> jedis.getConnectionFromSlot(0).sendCommand(Protocol.Command.CLUSTER, "1", "2"))
            .hasMessage(
                "ERR Unknown subcommand or wrong number of arguments for '1'. Try CLUSTER HELP.");
    assertThatThrownBy(
        () -> jedis.getConnectionFromSlot(0).sendCommand(Protocol.Command.CLUSTER, "SLOTS", "1"))
            .hasMessage(
                "ERR Unknown subcommand or wrong number of arguments for 'SLOTS'. Try CLUSTER HELP.");
  }
}
