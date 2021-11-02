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
package org.apache.geode.redis.internal.executor.sortedset;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_CURSOR;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.GeodeRedisServerRule;

public class ZScanIntegrationTest extends AbstractZScanIntegrationTest {
  String GREATER_THAN_LONG_MAX = "9_223_372_036_854_775_808";

  @ClassRule
  public static GeodeRedisServerRule server =
      new GeodeRedisServerRule();

  @Override
  public int getPort() {
    return server.getPort();
  }

  // Redis allows CURSOR values up to UNSIGNED_LONG_CAPACITY, but behaviour for CURSOR values
  // greater than Integer.MAX_VALUE is undefined, so we choose to return an error if a value greater
  // than Long.MAX_VALUE is passed
  @Test
  public void shouldReturnError_givenCursorGreaterThanLongMaxValue() {
    jedis.zadd(KEY, SCORE_ONE, MEMBER_ONE);

    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.ZSCAN, KEY,
        GREATER_THAN_LONG_MAX))
            .hasMessageContaining(ERROR_CURSOR);
  }
}
