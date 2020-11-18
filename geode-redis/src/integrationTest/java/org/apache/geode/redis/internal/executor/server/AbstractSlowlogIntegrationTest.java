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

package org.apache.geode.redis.internal.executor.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.util.Slowlog;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.RedisPortSupplier;

public abstract class AbstractSlowlogIntegrationTest implements RedisPortSupplier {

  private Jedis jedis;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @Before
  public void setUp() {
    jedis = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    jedis.close();
  }

  @Test
  public void shouldReturnEmptyArray_whenGetSubcommandSpecified() {
    List<Slowlog> actualResult = jedis.slowlogGet();

    assertThat(actualResult.isEmpty());
  }

  @Test
  public void shouldReturnEmptyArray_whenGetSubcommandSpecified_withLengthParameter() {
    List<Slowlog> actualResult = jedis.slowlogGet(200);

    assertThat(actualResult.isEmpty());
  }

  @Test
  public void shouldReturnEmptyArray_whenGetSubcommandSpecified_withNegativeLengthParameter() {
    List<Slowlog> actualResult = jedis.slowlogGet(-200);

    assertThat(actualResult.isEmpty());
  }

  @Test
  public void shouldNotThrowException_whenGetSubcommandSpecified_givenLongValue() {
    List<Slowlog> actualResult = jedis.slowlogGet(Long.MAX_VALUE);

    assertThat(actualResult.isEmpty());
  }

  @Test
  public void shouldReturnZero_whenLenSubcommandSpecified() {
    Long length = jedis.slowlogLen();

    assertThat(length).isEqualTo(0L);
  }

  @Test
  public void shouldReturnOK_whenResetSubcommandSpecified() {
    String response = jedis.slowlogReset();

    assertThat(response).isEqualTo("OK");
  }

  @Test
  public void shouldThrowException_givenAnyExtraParameterIsProvidedToReset() {
    assertThatThrownBy(
        () -> jedis.sendCommand(
            Protocol.Command.SLOWLOG, "RESET", "Superfluous"))
                .hasMessage(
                    "ERR Unknown subcommand or wrong number of arguments for 'RESET'. Try SLOWLOG HELP.");
  }

  @Test
  public void shouldThrowException_givenAnyExtraParameterIsProvidedToLenAParameter() {
    assertThatThrownBy(
        () -> jedis.sendCommand(
            Protocol.Command.SLOWLOG, "LEN", "Superfluous"))
                .hasMessage(
                    "ERR Unknown subcommand or wrong number of arguments for 'LEN'. Try SLOWLOG HELP.");
  }

  @Test
  public void shouldMatchCaseOfSentCommand_whenThrowingException() {
    assertThatThrownBy(
        () -> jedis.sendCommand(
            Protocol.Command.SLOWLOG, "lEn", "Superfluous"))
                .hasMessage(
                    "ERR Unknown subcommand or wrong number of arguments for 'lEn'. Try SLOWLOG HELP.");
  }

  @Test
  public void shouldThrowException_givenNonIntegerParameterToGET() {
    assertThatThrownBy(
        () -> jedis.sendCommand(
            Protocol.Command.SLOWLOG, "GET", "I am not a number"))
                .hasMessage("ERR value is not an integer or out of range");
  }

  @Test
  public void shouldThrowException_givenMoreThanThreeArgumentsAfterCommand() {
    assertThatThrownBy(
        () -> jedis.sendCommand(
            Protocol.Command.SLOWLOG, "secondArg", "thirdArg", "fourthArg"))
                .hasMessage(
                    "ERR Unknown subcommand or wrong number of arguments for 'secondArg'. Try SLOWLOG HELP.");
  }

  @Test
  public void shouldThrowException_givenUnknownSubcommand() {
    assertThatThrownBy(
        () -> jedis.sendCommand(
            Protocol.Command.SLOWLOG, "xlerb"))
                .hasMessage(
                    "ERR Unknown subcommand or wrong number of arguments for 'xlerb'. Try SLOWLOG HELP.");
  }

  @Test
  public void shouldThrowException_givenNoSubcommand() {
    assertThatThrownBy(
        () -> jedis.sendCommand(
            Protocol.Command.SLOWLOG))
                .hasMessage("ERR wrong number of arguments for 'slowlog' command");
  }
}
