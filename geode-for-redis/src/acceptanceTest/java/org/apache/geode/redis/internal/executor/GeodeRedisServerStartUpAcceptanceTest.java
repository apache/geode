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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;

public class GeodeRedisServerStartUpAcceptanceTest {

  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Test
  public void shouldReturnErrorMessage_givenSamePortAndAddress() throws IOException {

    int port = AvailablePortHelper.getRandomAvailableTCPPort();

    String startServerCommand = String.join(" ",
        "start server",
        "--server-port", "0",
        "--name", "same-port-and-address-server",
        "--geode-for-redis-bind-address", "localhost",
        "--geode-for-redis-port", String.valueOf(port));
    GfshExecution execution;

    try (Socket interferingSocket = new Socket()) {
      interferingSocket.bind(new InetSocketAddress("localhost", port));
      execution = GfshScript.of(startServerCommand)
          .expectFailure()
          .execute(gfshRule);
    }

    assertThat(execution.getOutputText()).containsIgnoringCase("address already in use");
  }

  @Test
  public void shouldReturnErrorMessage_givenSamePortAndAllAddresses() throws IOException {

    int port = AvailablePortHelper.getRandomAvailableTCPPort();

    String startServerCommand = String.join(" ",
        "start server",
        "--server-port", "0",
        "--name", "same-port-all-addresses-server",
        "--geode-for-redis-port", String.valueOf(port));
    GfshExecution execution;

    try (Socket interferingSocket = new Socket()) {
      interferingSocket.bind(new InetSocketAddress("0.0.0.0", port));
      execution = GfshScript.of(startServerCommand)
          .expectFailure()
          .execute(gfshRule);
    }

    assertThat(execution.getOutputText()).containsIgnoringCase("address already in use");
  }

  @Test
  public void shouldReturnErrorMessage_givenInvalidBindAddress() {

    String startServerCommand = String.join(" ",
        "start server",
        "--server-port", "0",
        "--name", "invalid-bind-server",
        "--geode-for-redis-bind-address", "1.1.1.1");
    GfshExecution execution;

    execution = GfshScript.of(startServerCommand)
        .expectFailure()
        .execute(gfshRule);

    assertThat(execution.getOutputText()).containsIgnoringCase(
        "The geode-for-redis-bind-address 1.1.1.1 is not a valid address for this machine");
  }
}
