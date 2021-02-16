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

package org.apache.geode.redis;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

public class RedisCommandArgumentsTestHelper {
  public static void assertExactNumberOfArgs(Jedis jedis, Protocol.Command command, int numArgs) {
    final int MAX_NUM_ARGS = 5; // currently enough for all implemented commands

    for (int i = 0; i <= MAX_NUM_ARGS; i++) {
      if (i != numArgs) {
        byte[][] args = buildArgs(i);
        assertThatThrownBy(() -> jedis.sendCommand(command, args))
            .hasMessageContaining("ERR wrong number of arguments for '"
                + command.toString().toLowerCase() + "' command");
      }
    }
  }

  public static void assertAtLeastNArgs(Jedis jedis, Protocol.Command command, int minNumArgs) {
    for (int i = 0; i < minNumArgs; i++) {
      byte[][] args = buildArgs(i);
      assertThatThrownBy(() -> jedis.sendCommand(command, args))
          .hasMessageContaining("ERR wrong number of arguments for '"
              + command.toString().toLowerCase() + "' command");
    }
  }

  public static void assertAtMostNArgs(Jedis jedis, Protocol.Command command, int maxNumArgs) {
    for (int i = maxNumArgs + 1; i <= 5; i++) {
      byte[][] args = buildArgs(i);
      assertThatThrownBy(() -> jedis.sendCommand(command, args))
          .hasMessageContaining("ERR wrong number of arguments for '"
              + command.toString().toLowerCase() + "' command");
    }
  }

  private static byte[][] buildArgs(int numArgs) {
    byte[][] args = new byte[numArgs][];

    if (numArgs == 0) {
      return args;
    }

    for (int i = 0; i < numArgs; i++) {
      args[i] = String.valueOf(i).getBytes();
    }

    return args;
  }
}
