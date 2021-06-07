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
package org.apache.geode.redis.internal.executor.sortedset;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_INVALID_ZADD_OPTION_NX_XX;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_A_VALID_FLOAT;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_ZADD_OPTION_TOO_MANY_INCR_PAIR;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class ZAddExecutor extends AbstractExecutor {

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    ZAddExecutorState zAddExecutorState = new ZAddExecutorState();
    RedisSortedSetCommands redisSortedSetCommands = context.getRedisSortedSetCommands();
    List<byte[]> commandElements = command.getProcessedCommand();
    Iterator<byte[]> commandIterator = commandElements.iterator();

    skipCommandAndKey(commandIterator);

    int optionsFoundCount = findAndValidateZAddOptions(command, commandIterator, zAddExecutorState);
    if (zAddExecutorState.exceptionMessage != null) {
      return RedisResponse.error(zAddExecutorState.exceptionMessage);
    }

    if (zAddExecutorState.incrFound) {
      byte[] result = (byte[]) redisSortedSetCommands.zadd(command.getKey(),
          new ArrayList<>(commandElements.subList(optionsFoundCount + 2, commandElements.size())),
          makeOptions(zAddExecutorState));
      if (result == null) {
        return RedisResponse.nil();
      }
      return RedisResponse.string(result);
    }

    return RedisResponse
        .integer((int) redisSortedSetCommands.zadd(command.getKey(),
            new ArrayList<>(commandElements.subList(optionsFoundCount + 2, commandElements.size())),
            makeOptions(zAddExecutorState)));
  }

  private void skipCommandAndKey(Iterator<byte[]> commandIterator) {
    commandIterator.next();
    commandIterator.next();
  }

  private int findAndValidateZAddOptions(Command command, Iterator<byte[]> commandIterator,
      ZAddExecutorState executorState) {
    boolean scoreFound = false;
    int optionsFoundCount = 0;

    while (commandIterator.hasNext() && !scoreFound) {
      String subCommandString = Coder.bytesToString(commandIterator.next()).toLowerCase();
      switch (subCommandString) {
        case "nx":
          executorState.nxFound = true;
          optionsFoundCount++;
          break;
        case "xx":
          executorState.xxFound = true;
          optionsFoundCount++;
          break;
        case "ch":
          executorState.chFound = true;
          optionsFoundCount++;
          break;
        case "incr":
          executorState.incrFound = true;
          optionsFoundCount++;
          break;
        default:
          try {
            Double.valueOf(subCommandString);
          } catch (NumberFormatException nfe) {
            executorState.exceptionMessage = ERROR_NOT_A_VALID_FLOAT;
          }
          scoreFound = true;
      }
    }
    if ((command.getProcessedCommand().size() - optionsFoundCount - 2) % 2 != 0) {
      executorState.exceptionMessage = ERROR_SYNTAX;
    } else if (executorState.nxFound && executorState.xxFound) {
      executorState.exceptionMessage = ERROR_INVALID_ZADD_OPTION_NX_XX;
    } else if (executorState.incrFound
        && command.getProcessedCommand().size() - optionsFoundCount - 2 > 2) {
      executorState.exceptionMessage = ERROR_ZADD_OPTION_TOO_MANY_INCR_PAIR;
    }
    return optionsFoundCount;
  }

  private ZAddOptions makeOptions(ZAddExecutorState executorState) {
    ZAddOptions.Exists existsOption = ZAddOptions.Exists.NONE;

    if (executorState.nxFound) {
      existsOption = ZAddOptions.Exists.NX;
    }
    if (executorState.xxFound) {
      existsOption = ZAddOptions.Exists.XX;
    }
    return new ZAddOptions(existsOption, executorState.chFound, executorState.incrFound);
  }

  private static class ZAddExecutorState {
    private boolean nxFound = false;
    private boolean xxFound = false;
    private boolean chFound = false;
    private boolean incrFound = false;
    private String exceptionMessage = null;
  }
}
