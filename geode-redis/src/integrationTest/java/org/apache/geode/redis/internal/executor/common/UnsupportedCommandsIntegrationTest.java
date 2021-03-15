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

package org.apache.geode.redis.internal.executor.common;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_UNKNOWN_COMMAND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.SafeEncoder;

import org.apache.geode.redis.GeodeRedisServerRule;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.RedisPortSupplier;


public class UnsupportedCommandsIntegrationTest
    implements RedisPortSupplier {

  protected static Jedis jedis;

  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @ClassRule
  public static GeodeRedisServerRule server = new GeodeRedisServerRule(false);

  @Override
  public int getPort() {
    return server.getPort();
  }

  @Before
  public void setUp() {
    jedis = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @Test
  public void shouldNotError_givenCallToUnsupportedCommandAndSystemPropertySet() {
    final String NEW_VALUE = "new value";

    System.setProperty("enable-unsupported-commands", "true");

    jedis.set("key", "value");
    jedis.getSet("key", NEW_VALUE);

    String actual = jedis.get("key");

    assertThat(actual).isEqualTo(NEW_VALUE);
  }

  @Test
  public void shouldReturnUnknownCommandMessage_givenCallToUnsupportedCommandAndSystemPropertyNotSet() {
    final String KEY = "key";
    final String NEW_VALUE = "changed value";
    final String EXPECTED_ERROR_MSG =
        String.format(ERROR_UNKNOWN_COMMAND, "GETSET", "`" + KEY + "`", NEW_VALUE);

    jedis.set(KEY, "value");

    assertThatThrownBy(
        () -> jedis.getSet(KEY, NEW_VALUE))
            .hasMessageContaining(EXPECTED_ERROR_MSG);
  }

  @Test
  public void shouldReturnUnknownCommandMessage_givenCallToInternalCommandAndSystemPropertyNotSet() {
    final String TEST_PARAMETER = "this is only a test";
    final String EXPECTED_ERROR_MSG =
        String.format(ERROR_UNKNOWN_COMMAND, InternalCommands.INTERNALPTTL,
            "`" + TEST_PARAMETER + "`");

    assertThatThrownBy(
        () -> jedis.sendCommand(InternalCommands.INTERNALPTTL, TEST_PARAMETER))
            .hasMessageContaining(EXPECTED_ERROR_MSG);
  }

  @Test
  public void shouldReturnUnknownCommandMessage_givenCallToInternalCommandAndSystemPropertySet() {
    final String TEST_PARAMETER = " this is only a test";
    final String EXPECTED_ERROR_MSG =
        String.format(ERROR_UNKNOWN_COMMAND, InternalCommands.INTERNALTYPE,
            "`" + TEST_PARAMETER + "`");

    System.setProperty("enable-unsupported-commands", "true");

    assertThatThrownBy(
        () -> jedis.sendCommand(InternalCommands.INTERNALTYPE, TEST_PARAMETER))
            .hasMessageContaining(EXPECTED_ERROR_MSG);
  }

  @After
  public void tearDown() {
    System.setProperty("enable-unsupported-commands", "true");
    jedis.flushAll();
    System.clearProperty("enable-unsupported-commands");
  }

  @AfterClass
  public static void afterClass() {
    jedis.close();
  }

  private enum InternalCommands implements ProtocolCommand {
    INTERNALPTTL("INTERNALPTTL"),
    INTERNALTYPE("INTERNALTYPE");

    private final byte[] raw;

    InternalCommands(String command) {
      this.raw = SafeEncoder.encode(command);
    }

    @Override
    public byte[] getRaw() {
      return raw;
    }
  }
}
