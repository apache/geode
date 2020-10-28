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

import static org.apache.geode.redis.internal.RedisConstants.ERROR_INVALID_DB_INDEX;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SELECT_CLUSTER_MODE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.RedisPortSupplier;

public abstract class AbstractSelectIntegrationTest implements RedisPortSupplier {

  protected static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  protected static Jedis jedis;

  @Before
  public void setUp() {
    jedis = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    jedis.close();
  }

  @Test
  public void givenLessThanTwoArguments_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SELECT))
        .hasMessage("ERR wrong number of arguments for 'select' command");
  }

  @Test
  public void givenMoreThanTwoArguments_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SELECT, "notALong", "extraArg"))
        .hasMessage("ERR wrong number of arguments for 'select' command");
  }

  @Test
  public void givenIndexArgumentIsNotALong_returnsInvalidDBIndexError() {
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SELECT, "notALong"))
        .hasMessageContaining(ERROR_INVALID_DB_INDEX);
  }

  @Test
  public void givenIndexArgumentWouldOverflow_returnsInvalidDBIndexError() {
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SELECT, "9223372036854775808"))
        .hasMessageContaining(ERROR_INVALID_DB_INDEX);
  }

  @Test
  public void givenIndexOfZero_returnsOk() {
    assertThat(jedis.select(0)).isEqualTo("OK");
  }

  @Test
  public void givenAnyValidIndexOtherThanZero_returnsClusterModeError() {
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SELECT, "9223372036854775807"))
        .hasMessageContaining(ERROR_SELECT_CLUSTER_MODE);
  }
}
