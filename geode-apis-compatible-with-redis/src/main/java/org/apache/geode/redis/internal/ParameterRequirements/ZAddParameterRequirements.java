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

import static org.apache.geode.redis.internal.RedisConstants.ERROR_INVALID_ZADD_OPTION_NX_XX;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_A_VALID_FLOAT;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;

import java.util.Iterator;
import java.util.List;

import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class ZAddParameterRequirements implements ParameterRequirements {
  @Override
  public void checkParameters(Command command, ExecutionHandlerContext context) {
    int optionsFoundCount = 0;
    boolean nxFound = false, xxFound = false;
    boolean scoreFound = false;
    String exceptionMessage = null;
    int numberOfArguments = command.getProcessedCommand().size();

    List<byte[]> commandElements = command.getProcessedCommand();
    Iterator<byte[]> commandIterator = commandElements.iterator();
    commandIterator.next(); // Skip over command
    commandIterator.next(); // and key

    while (commandIterator.hasNext()) {
      byte[] subcommand = commandIterator.next();
      String subCommandString = Coder.bytesToString(subcommand).toLowerCase();
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
        default:
          try {
            Double.valueOf(subCommandString);
            scoreFound = true;
            break;
          } catch (NumberFormatException nfe) {
            exceptionMessage = ERROR_NOT_A_VALID_FLOAT;
          }
      }
      if (scoreFound || exceptionMessage != null) {
        break;
      }
      optionsFoundCount++;
    }

    if ((numberOfArguments - optionsFoundCount - 2) % 2 != 0) {
      exceptionMessage = ERROR_SYNTAX;
    } else if (nxFound && xxFound) {
      exceptionMessage = ERROR_INVALID_ZADD_OPTION_NX_XX;
    }

    if (exceptionMessage != null) {
      throw new RedisParametersMismatchException(exceptionMessage);
    }
  }

}
