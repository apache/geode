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

package org.apache.geode.redis.internal.executor;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.RedisPortSupplier;

public abstract class AbstractUnknownIntegrationTest implements RedisPortSupplier {

  private Jedis jedis;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @Before
  public void setUp() {
    jedis = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    jedis.flushAll();
    jedis.close();
  }

  @Test
  public void givenUnknownCommand_returnsUnknownCommandError() {
    assertThatThrownBy(() -> jedis.sendCommand("fhqwhgads"::getBytes))
        .hasMessage("ERR unknown command `fhqwhgads`, with args beginning with: ");
  }

  @Test
  public void givenUnknownCommand_withArguments_returnsUnknownCommandErrorWithArgumentsListed() {
    assertThatThrownBy(() -> jedis.sendCommand("fhqwhgads"::getBytes, "EVERYBODY", "TO THE LIMIT"))
        .hasMessage(
            "ERR unknown command `fhqwhgads`, with args beginning with: `EVERYBODY`, `TO THE LIMIT`, ");
  }

  @Test
  public void givenUnknownCommand_withEmptyStringArgument_returnsUnknownCommandErrorWithArgumentsListed() {
    assertThatThrownBy(() -> jedis.sendCommand("fhqwhgads"::getBytes, "EVERYBODY", ""))
        .hasMessage("ERR unknown command `fhqwhgads`, with args beginning with: `EVERYBODY`, ``, ");
  }

  @Test // HELLO is not a recognized command until Redis 6.0.0
  public void givenHelloCommand_returnsUnknownCommandErrorWithArgumentsListed() {
    assertThatThrownBy(() -> jedis.sendCommand("HELLO"::getBytes))
        .hasMessage("ERR unknown command `HELLO`, with args beginning with: ");
  }

  @Test
  public void givenInternalSMembersCommand_returnsUnknownCommandErrorWithArgumentsListed() {
    assertThatThrownBy(
        () -> jedis.sendCommand("INTERNALSMEMBERS"::getBytes, "something", "somethingElse"))
            .hasMessage(
                "ERR unknown command `INTERNALSMEMBERS`, with args beginning with: `something`, `somethingElse`, ");
  }

  @Test
  public void givenInternalPTTLCommand_returnsUnknownCommandErrorWithArgumentsListed() {
    assertThatThrownBy(() -> jedis.sendCommand("INTERNALPTTL"::getBytes, "something"))
        .hasMessage("ERR unknown command `INTERNALPTTL`, with args beginning with: `something`, ");
  }

  @Test
  public void givenInternalTypeCommand_returnsUnknownCommandErrorWithArgumentsListed() {
    assertThatThrownBy(() -> jedis.sendCommand("INTERNALTYPE"::getBytes, "something"))
        .hasMessage("ERR unknown command `INTERNALTYPE`, with args beginning with: `something`, ");
  }
}
