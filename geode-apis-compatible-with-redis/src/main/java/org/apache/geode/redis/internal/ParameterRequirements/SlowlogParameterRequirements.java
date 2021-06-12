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

package org.apache.geode.redis.internal.ParameterRequirements;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_UNKNOWN_SLOWLOG_SUBCOMMAND;

import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class SlowlogParameterRequirements implements ParameterRequirements {
  @Override
  public void checkParameters(Command command, ExecutionHandlerContext context) {
    int numberOfArguments = command.getProcessedCommand().size();

    if (numberOfArguments < 2) {
      throw new RedisParametersMismatchException(command.wrongNumberOfArgumentsErrorMessage());
    } else if (numberOfArguments == 2) {
      confirmKnownSubcommands(command);
    } else if (numberOfArguments == 3) {
      confirmArgumentsToGetSubcommand(command);
    } else { // numberOfArguments > 3
      throw new RedisParametersMismatchException(
          String.format(ERROR_UNKNOWN_SLOWLOG_SUBCOMMAND, command.getStringKey()));
    }
  }

  private void confirmKnownSubcommands(Command command) {
    if (!command.getStringKey().toLowerCase().equals("reset") &&
        !command.getStringKey().toLowerCase().equals("len") &&
        !command.getStringKey().toLowerCase().equals("get")) {
      throw new RedisParametersMismatchException(
          String.format(ERROR_UNKNOWN_SLOWLOG_SUBCOMMAND, command.getStringKey()));
    }
  }

  private void confirmArgumentsToGetSubcommand(Command command) {
    if (!command.getStringKey().toLowerCase().equals("get")) {
      throw new RedisParametersMismatchException(
          String.format(ERROR_UNKNOWN_SLOWLOG_SUBCOMMAND, command.getStringKey()));
    }
    try {
      Long.parseLong(new String(command.getProcessedCommand().get(2)));
    } catch (NumberFormatException nex) {
      throw new RedisParametersMismatchException(ERROR_NOT_INTEGER);
    }
  }

}
