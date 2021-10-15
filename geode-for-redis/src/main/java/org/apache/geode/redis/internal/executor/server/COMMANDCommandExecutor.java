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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.geode.redis.internal.RedisCommandType;
import org.apache.geode.redis.internal.executor.CommandExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
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
  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {

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
}
