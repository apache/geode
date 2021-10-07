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

package org.apache.geode.redis.internal.parameters;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_UNKNOWN_SLOWLOG_SUBCOMMAND;
import static org.apache.geode.redis.internal.netty.Coder.bytesToLong;
import static org.apache.geode.redis.internal.netty.Coder.bytesToString;
import static org.apache.geode.redis.internal.netty.Coder.equalsIgnoreCaseBytes;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bGET;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bLEN;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bRESET;

import java.util.function.Consumer;

import org.apache.geode.redis.internal.netty.Command;

public class SlowlogParameterRequirements {

  public static Consumer<Command> checkParameters() {
    return command -> {
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
    };
  }

  private static void confirmKnownSubcommands(Command command) {
    byte[] bytes = command.getBytesKey();
    if (!equalsIgnoreCaseBytes(bytes, bRESET) &&
        !equalsIgnoreCaseBytes(bytes, bLEN) &&
        !equalsIgnoreCaseBytes(bytes, bGET)) {
      throw new RedisParametersMismatchException(
          String.format(ERROR_UNKNOWN_SLOWLOG_SUBCOMMAND, bytesToString(bytes)));
    }
  }

  private static void confirmArgumentsToGetSubcommand(Command command) {
    byte[] bytes = command.getBytesKey();
    if (!equalsIgnoreCaseBytes(bytes, bGET)) {
      throw new RedisParametersMismatchException(
          String.format(ERROR_UNKNOWN_SLOWLOG_SUBCOMMAND, bytesToString(bytes)));
    }
    try {
      bytesToLong(command.getProcessedCommand().get(2));
    } catch (NumberFormatException nex) {
      throw new RedisParametersMismatchException(ERROR_NOT_INTEGER);
    }
  }

}
