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

import static org.apache.geode.redis.internal.RedisConstants.ERROR_INVALID_ZADD_OPTION_GT_LT_NX;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_INVALID_ZADD_OPTION_NX_XX;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;

import java.util.Iterator;
import java.util.List;

import org.apache.logging.log4j.Logger;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class ZAddParameterRequirements implements ParameterRequirements {
  @Override
  public void checkParameters(Command command, ExecutionHandlerContext context) {
    int numberOfArguments = command.getProcessedCommand().size();

    if (numberOfArguments < 4) {
      throw new RedisParametersMismatchException(command.wrongNumberOfArgumentsErrorMessage());
    }

    int optionsFoundCount = confirmKnownSubcommands(command);

    if ((numberOfArguments - optionsFoundCount - 2) % 2 != 0) {
      throw new RedisParametersMismatchException(command.wrongNumberOfArgumentsErrorMessage());
    }
  }

  private int confirmKnownSubcommands(Command command) {
    int optionsFoundCount = 0;
    boolean nxFound = false, xxFound = false, gtFound = false, ltFound = false;

    List<ByteArrayWrapper> commandElements = command.getProcessedCommandWrappers();
    Iterator<ByteArrayWrapper> commandIterator = commandElements.iterator();
    commandIterator.next(); // Skip past command
    commandIterator.next(); // and key

    while (commandIterator.hasNext()) {
      ByteArrayWrapper subcommand = commandIterator.next();
      String subCommandString = subcommand.toString().toLowerCase();
      try {
        Double.valueOf(subCommandString);
        break;
      } catch (NumberFormatException nfe) {
        System.out.println("Got exception on: " + subCommandString);
        nfe.printStackTrace();
      }
      switch (subCommandString) {
        case "ch":
          break;
        case "incr":
          break;
        case "nx":
          nxFound = true;
          break;
        case "xx":
          xxFound = true;
          break;
        case "gt":
          gtFound = true;
          break;
        case "lt":
          ltFound = true;
          break;
        default:
          throw new RedisParametersMismatchException(String.format(ERROR_SYNTAX));
      }
      optionsFoundCount++;
    }
    // Validate flag combos
    if (nxFound && xxFound) {
      throw new RedisParametersMismatchException(
          String.format(ERROR_INVALID_ZADD_OPTION_NX_XX));
    }
    if (gtFound && ltFound) {
      throw new RedisParametersMismatchException(
          String.format(ERROR_INVALID_ZADD_OPTION_GT_LT_NX));
    }
    if ((gtFound || ltFound) && nxFound) {
      throw new RedisParametersMismatchException(
          String.format(ERROR_INVALID_ZADD_OPTION_GT_LT_NX));
    }

    return optionsFoundCount;
  }
}
