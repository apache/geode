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

package org.apache.geode.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.Socket;
import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public class RawTcpDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule cluster = new RedisClusterStartupRule();

  @BeforeClass
  public static void setUp() throws IOException {
    cluster.startRedisVM(0);
  }

  @Test
  public void ignoreZeroLengthArrays() throws Exception {
    Socket redisSocket = new Socket("localhost", cluster.getRedisPort(0));

    byte[] rawBytes = new byte[] {
        '*', '0', 0x0d, 0x0a,
        '*', '0', 0x0d, 0x0a,
        '*', '1', 0x0d, 0x0a,
        '$', '4', 0x0d, 0x0a,
        'P', 'I', 'N', 'G', 0x0d, 0x0a,
    };

    redisSocket.getOutputStream().write(rawBytes);

    byte[] inputBuffer = new byte[1024];
    int n = redisSocket.getInputStream().read(inputBuffer);
    String result = new String(Arrays.copyOfRange(inputBuffer, 0, n)).toLowerCase();

    assertThat(result).contains("pong");
  }

  @Test
  public void respondWithImproperlyFormattedError_whenSendingMalformedString() throws Exception {
    Socket redisSocket = new Socket("localhost", cluster.getRedisPort(0));

    byte[] rawBytes = new byte[] {
        '$', '1', 0x0d, 0x0a, 'a', 'b', 'c', 0x0d, 0x0a
    };

    redisSocket.getOutputStream().write(rawBytes);

    byte[] inputBuffer = new byte[1024];
    int n = redisSocket.getInputStream().read(inputBuffer);

    String result = new String(Arrays.copyOfRange(inputBuffer, 0, n)).toLowerCase();
    assertThat(result)
        .contains("the command received by geoderedisserver was improperly formatted");
  }

}
