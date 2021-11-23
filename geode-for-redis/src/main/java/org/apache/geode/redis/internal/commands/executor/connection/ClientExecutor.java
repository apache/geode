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
package org.apache.geode.redis.internal.commands.executor.connection;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_INVALID_CLIENT_NAME;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_UNKNOWN_CLIENT_SUBCOMMAND;
import static org.apache.geode.redis.internal.netty.Coder.bytesToString;
import static org.apache.geode.redis.internal.netty.Coder.toUpperCaseBytes;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.GETNAME;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.SETNAME;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.redis.internal.commands.Command;
import org.apache.geode.redis.internal.commands.executor.CommandExecutor;
import org.apache.geode.redis.internal.commands.executor.RedisResponse;
import org.apache.geode.redis.internal.commands.parameters.RedisParametersMismatchException;
import org.apache.geode.redis.internal.netty.Client;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class ClientExecutor implements CommandExecutor {
  @Immutable
  private static final List<String> supportedSubcommands =
      Collections.unmodifiableList(Arrays.asList("SETNAME", "GETNAME"));

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> args = command.getProcessedCommand();
    byte[] subcommand = args.get(1);
    byte[] upperCaseSubcommand = toUpperCaseBytes(subcommand);

    Client client = context.getClient();
    if (Arrays.equals(upperCaseSubcommand, SETNAME)) {
      checkNumberOfArguments(subcommand, 3, args);
      byte[] clientName = args.get(2);
      validateClientName(clientName);
      client.setName(clientName);
      return RedisResponse.ok();
    } else if (Arrays.equals(upperCaseSubcommand, GETNAME)) {
      checkNumberOfArguments(subcommand, 2, args);
      return RedisResponse.bulkString(client.getName());
    } else {
      return RedisResponse.error(
          String.format(ERROR_UNKNOWN_CLIENT_SUBCOMMAND, bytesToString(subcommand)));
    }
  }

  private void checkNumberOfArguments(byte[] subcommand, int expectedArguments, List<byte[]> args) {
    if (args.size() != expectedArguments) {
      throw new RedisParametersMismatchException(
          String.format(ERROR_UNKNOWN_CLIENT_SUBCOMMAND, bytesToString(subcommand)));
    }
  }

  private void validateClientName(byte[] clientName) {
    for (byte clientChar : clientName) {
      if (clientChar < '!' || clientChar > '~') {
        throw new RedisParametersMismatchException(ERROR_INVALID_CLIENT_NAME);
      }
    }
  }

  public static List<String> getSupportedSubcommands() {
    return supportedSubcommands;
  }
}
