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

import static org.apache.geode.redis.internal.RedisConstants.ERROR_SELECT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.GeodeRedisServerRule;

public class SelectIntegrationTest extends AbstractSelectIntegrationTest {

  @ClassRule
  public static GeodeRedisServerRule server = new GeodeRedisServerRule();

  @Override
  public int getPort() {
    return server.getPort();
  }

  // our SELECT implementation diverges from Redis and only supports DB 0
  @Test
  public void givenAnyDBIndexOtherThanZero_returnsSelectError() {
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SELECT, "9223372036854775807"))
        .hasMessageContaining(ERROR_SELECT);
  }
}
