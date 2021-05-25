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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class ZAddExecutor extends AbstractExecutor {
  private final ZAddExecutorState zAddExecutorState = new ZAddExecutorState();

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    zAddExecutorState.initialize();
    RedisSortedSetCommands redisSortedSetCommands = context.getRedisSortedSetCommands();
    List<byte[]> commandElements = command.getProcessedCommand();
    Iterator<byte[]> commandIterator = commandElements.iterator();

    skipCommandAndKey(commandIterator);

    byte[] firstScore = findAndValidateZAddOptions(command, commandIterator, zAddExecutorState);
    if (zAddExecutorState.exceptionMessage != null) {
      return RedisResponse.error(zAddExecutorState.exceptionMessage);
    }

    List<byte[]> scoresAndMembersToAdd = getScoresAndMembers(firstScore, commandIterator,
        zAddExecutorState);
    if (zAddExecutorState.exceptionMessage != null) {
      return RedisResponse.error(zAddExecutorState.exceptionMessage);
    }

    return RedisResponse
        .integer(redisSortedSetCommands.zadd(command.getKey(), scoresAndMembersToAdd,
            makeOptions(zAddExecutorState)));
  }

  private void skipCommandAndKey(Iterator<byte[]> commandIterator) {
    commandIterator.next();
    commandIterator.next();
  }

  private byte[] findAndValidateZAddOptions(Command command, Iterator<byte[]> commandIterator,
      ZAddExecutorState executorState) {
    boolean scoreFound = false;
    byte[] next = new byte[0];

    while (commandIterator.hasNext() && !scoreFound) {
      next = commandIterator.next();
      String subCommandString = Coder.bytesToString(next).toLowerCase();
      switch (subCommandString) {
        case "nx":
          executorState.nxFound = true;
          executorState.optionsFoundCount++;
          break;
        case "xx":
          executorState.xxFound = true;
          executorState.optionsFoundCount++;
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
    if ((command.getProcessedCommand().size() - executorState.optionsFoundCount - 2) % 2 != 0) {
      executorState.exceptionMessage = ERROR_SYNTAX;
    } else if (executorState.nxFound && executorState.xxFound) {
      executorState.exceptionMessage = ERROR_INVALID_ZADD_OPTION_NX_XX;
    }
    return next;
  }

  private List<byte[]> getScoresAndMembers(byte[] firstScore, Iterator<byte[]> commandIterator,
      ZAddExecutorState executorState) {
    List<byte[]> scoresAndMembersToAdd = new ArrayList<>();

    // Already validated there's at least one pair
    scoresAndMembersToAdd.add(firstScore);
    scoresAndMembersToAdd.add(commandIterator.next());

    // Any more?
    while (commandIterator.hasNext()) {
      byte[] score = commandIterator.next();
      try {
        Double.valueOf(Coder.bytesToString(score));
      } catch (NumberFormatException nfe) {
        executorState.exceptionMessage = ERROR_NOT_A_VALID_FLOAT;
        return null;
      }
      scoresAndMembersToAdd.add(score);

      // We already validated even number of remaining params
      byte[] member = commandIterator.next();
      scoresAndMembersToAdd.add(member);
    }
    return scoresAndMembersToAdd;
  }

  private ZAddOptions makeOptions(ZAddExecutorState executorState) {
    ZAddOptions.Exists existsOption = ZAddOptions.Exists.NONE;

    if (executorState.nxFound) {
      existsOption = ZAddOptions.Exists.NX;
    }
    if (executorState.xxFound) {
      existsOption = ZAddOptions.Exists.XX;
    }
    return new ZAddOptions(existsOption);
  }

  static class ZAddExecutorState {
    public int optionsFoundCount = 0;
    public boolean nxFound = false, xxFound = false;
    public String exceptionMessage = null;

    public void initialize() {
      optionsFoundCount = 0;
      nxFound = false;
      xxFound = false;
      exceptionMessage = null;
    }
  }
}
