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
package org.apache.geode.redis.internal.executor.connection;

import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.RedisIntegrationTest;

public abstract class AbstractClientIntegrationTest implements RedisIntegrationTest {
  private Jedis jedis;

  @Before
  public void setUp() {
    jedis = new Jedis(BIND_ADDRESS, getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    jedis.close();
  }

  @Test
  public void clientWithInvalidSubcommand_returnError() {
    String invalidSubcommand = "subcommand";
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.CLIENT, invalidSubcommand))
        .hasMessageContaining("ERR Unknown subcommand or wrong number of arguments for '"
            + invalidSubcommand + "'. Try CLIENT HELP.");
  }

  @Test
  public void clientWithNoSubcommand_returnError() {
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.CLIENT))
        .hasMessageContaining("ERR wrong number of arguments for 'client' command");
  }

  @Test
  public void clientSetName_withNoArguments_returnError() {
    String subcommand = "SeTnAmE";
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.CLIENT, subcommand))
        .hasMessageContaining("ERR Unknown subcommand or wrong number of arguments for '"
            + subcommand + "'. Try CLIENT HELP.");
  }

  @Test
  public void clientSetName_withTooManyArguments_returnError() {
    String subcommand = "SeTnAmE";
    assertThatThrownBy(
        () -> jedis.sendCommand(Protocol.Command.CLIENT, subcommand, "AAAA", "BBBBBB"))
            .hasMessageContaining("ERR Unknown subcommand or wrong number of arguments for '"
                + subcommand + "'. Try CLIENT HELP.");
  }

  @Test
  public void clientSetName_withSpace_returnError() {
    String clientName = " ";
    assertThatThrownBy(() -> jedis.clientSetname(clientName)).hasMessageContaining(
        "ERR Client names cannot contain spaces, newlines or special characters.");
  }

  @Test
  public void clientSetName_withNewLine_returnError() {
    String clientName = "\n";
    assertThatThrownBy(() -> jedis.clientSetname(clientName)).hasMessageContaining(
        "ERR Client names cannot contain spaces, newlines or special characters.");
  }

  @Test
  public void clientSetName_withInvalidCharacter_returnError() {
    byte[] clientName = {0};
    assertThatThrownBy(() -> jedis.clientSetname(clientName)).hasMessageContaining(
        "ERR Client names cannot contain spaces, newlines or special characters.");
  }

  @Test
  public void clientGetName_withTooManyArguments_returnError() {
    String subcommand = "GeTNaMe";
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.CLIENT, subcommand, "AAAA"))
        .hasMessageContaining("ERR Unknown subcommand or wrong number of arguments for '"
            + subcommand + "'. Try CLIENT HELP.");
  }

  @Test
  public void clientGetName_withNoNameSet_returnNullResponse() {
    assertThat(jedis.clientGetname()).isNull();
  }

  @Test
  public void clientSetName_returnsOk() {
    assertThat(jedis.clientSetname("Name")).isEqualTo("OK");
  }

  @Test
  public void clientSetName_setsName() {
    String clientName = "Name";
    jedis.clientSetname(clientName);
    assertThat(jedis.clientGetname()).isEqualTo(clientName);
  }

  @Test
  public void clientSetName_canOverwriteName() {
    String clientName1 = "Name";
    jedis.clientSetname(clientName1);
    assertThat(jedis.clientGetname()).isEqualTo(clientName1);

    String clientName2 = "SecondName";
    jedis.clientSetname(clientName2);
    assertThat(jedis.clientGetname()).isEqualTo(clientName2);
  }

  @Test
  public void clientSetName_withEmptyString_setNameToNull() {
    String clientName = "";
    jedis.clientSetname(clientName);
    assertThat(jedis.clientGetname()).isNull();
  }
}
