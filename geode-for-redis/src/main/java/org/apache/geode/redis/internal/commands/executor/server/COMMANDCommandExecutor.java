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

package org.apache.geode.redis.internal.commands.executor.server;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_UNKNOWN_COMMAND_COMMAND_SUBCOMMAND;
import static org.apache.geode.redis.internal.netty.Coder.bytesToString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.redis.internal.commands.Command;
import org.apache.geode.redis.internal.commands.RedisCommandType;
import org.apache.geode.redis.internal.commands.executor.CommandExecutor;
import org.apache.geode.redis.internal.commands.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

/**
 * This class implements execution for the redis COMMAND command.
 * For clarity and to avoid this class name from getting mixed
 * up with the interface name it implements, both COMMAND and Command
 * are in the class name.
 * COMMAND refers to the redis command being implemented.
 * CommandExecutor refers to the interface being implemented.
 */
public class COMMANDCommandExecutor implements CommandExecutor {
  @Immutable
  private static final List<String> supportedSubcommands =
      Collections.unmodifiableList(Arrays.asList("(no subcommand)"));

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> args = command.getProcessedCommand();

    if (args.size() == 1) {
      return executeNoSubcommand(context);
    }

    // No subcommands are set for command so defaults to error response
    byte[] subcommand = args.get(1);
    return RedisResponse.error(
        String.format(ERROR_UNKNOWN_COMMAND_COMMAND_SUBCOMMAND, bytesToString(subcommand)));
  }

  private RedisResponse executeNoSubcommand(ExecutionHandlerContext context) {
    List<Object> response = new ArrayList<>();

    for (RedisCommandType type : RedisCommandType.values()) {
      if (type.isInternal()
          || type.isUnknown()
          || (type.isUnsupported() && !context.allowUnsupportedCommands())
          || type == RedisCommandType.QUIT) {
        continue;
      }

      List<Object> oneCommand = new ArrayList<>();
      oneCommand.add(type.name().toLowerCase());
      oneCommand.add(type.arity());
      oneCommand.add(type.flags().stream()
          .map(f -> f.name().toLowerCase()).collect(Collectors.toList()));
      oneCommand.add(type.firstKey());
      oneCommand.add(type.lastKey());
      oneCommand.add(type.step());

      response.add(oneCommand);
    }

    return RedisResponse.array(response, false);
  }

  public static List<String> getSupportedSubcommands() {
    return supportedSubcommands;
  }
}
